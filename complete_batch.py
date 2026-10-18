import argparse
import hashlib
import os
import uuid

from datetime import date, timedelta
from pathlib import Path

import psycopg
from psycopg import sql
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

DATASET = "recon_batch_pack_v1"
SOURCE_PREFIX = "synthetic_pack_v1"

DATABASE_CHECKPOINT_BYTES = 350_000_000


# Daily source records are staged first.
# bucket determines reproducible synthetic scenarios.
CREATE_DAY_ORDERS = """
CREATE TEMP TABLE day_orders ON COMMIT DROP AS
SELECT
    o.*,
    (
        ('x' || SUBSTR(MD5(o.order_id), 1, 8))
        ::BIT(32)::BIGINT %% 10000
    ) AS bucket
FROM batch.orders AS o
WHERE o.order_id >= %s
  AND o.order_id < %s
  AND LEFT(o.order_id, 7) = 'SYN_V2_';
"""


CREATE_PAYMENTS = """
CREATE TEMP TABLE stage_payments ON COMMIT DROP AS
WITH base AS (
    SELECT
        'SYN_PAY_' || order_id AS payment_id,
        order_id,

        'SYN_STL_'
            || TO_CHAR(%s::DATE, 'YYYYMMDD')
            || '_' || merchant_id AS settlement_id,

        %s::DATE AS business_date,
        source_updated_at AS captured_at,

        order_amount_minor
            + CASE
                WHEN bucket >= 50 AND bucket < 100
                THEN 100
                ELSE 0
              END AS amount_minor

    FROM day_orders

    -- Deliberately omit some payments despite PAID order status.
    WHERE order_status = 'PAID'
      AND bucket >= 50
),
fees AS (
    SELECT
        *,
        ROUND(amount_minor::NUMERIC * 0.02)::BIGINT AS fee_minor
    FROM base
)
SELECT
    *,
    ROUND(fee_minor::NUMERIC * 0.18)::BIGINT AS tax_minor
FROM fees;
"""


CREATE_REFUNDS = """
CREATE TEMP TABLE stage_refunds ON COMMIT DROP AS
SELECT
    'SYN_REF_' || p.payment_id AS refund_id,
    p.payment_id,
    p.captured_at + INTERVAL '1 hour' AS refunded_at,
    p.amount_minor
FROM stage_payments AS p
JOIN day_orders AS o
  ON o.order_id = p.order_id
WHERE o.bucket >= 100
  AND o.bucket < 300;
"""


CREATE_SETTLEMENTS = """
CREATE TEMP TABLE stage_settlements ON COMMIT DROP AS
WITH totals AS (
    SELECT
        p.settlement_id,
        o.merchant_id,
        p.business_date,
        p.business_date + 2 AS settlement_date,

        SUM(p.amount_minor)::BIGINT AS gross_amount_minor,
        SUM(COALESCE(r.amount_minor, 0))::BIGINT
            AS refund_amount_minor,

        SUM(p.fee_minor)::BIGINT AS fee_amount_minor,
        SUM(p.tax_minor)::BIGINT AS tax_amount_minor

    FROM stage_payments AS p
    JOIN day_orders AS o
      ON o.order_id = p.order_id
    LEFT JOIN stage_refunds AS r
      ON r.payment_id = p.payment_id

    GROUP BY
        p.settlement_id,
        o.merchant_id,
        p.business_date
)
SELECT
    *,
    gross_amount_minor
        - refund_amount_minor
        - fee_amount_minor
        - tax_amount_minor

        -- Inject a ₹1 settlement calculation difference.
        + CASE
            WHEN (
                ('x' || SUBSTR(
                    MD5(settlement_id || ':settlement'), 1, 8
                ))::BIT(32)::BIGINT % 100
            ) = 0
            THEN 100
            ELSE 0
          END AS net_amount_minor
FROM totals;
"""


CREATE_BANK_CREDITS = """
CREATE TEMP TABLE stage_bank_credits ON COMMIT DROP AS
WITH marked AS (
    SELECT
        *,
        (
            ('x' || SUBSTR(
                MD5(settlement_id || ':bank'), 1, 8
            ))::BIT(32)::BIGINT % 100
        ) AS bank_bucket
    FROM stage_settlements
)
SELECT
    'SYN_BANK_' || settlement_id AS bank_entry_id,
    settlement_id,
    settlement_date + 1 AS credited_on,

    GREATEST(
        net_amount_minor
            - CASE WHEN bank_bucket = 1 THEN 100 ELSE 0 END,
        0
    )::BIGINT AS amount_minor

FROM marked

-- Deliberately omit some expected bank credits.
WHERE bank_bucket <> 0;
"""


# Load settlements before payments to satisfy foreign keys.
TABLES = [
    (
        "settlements",
        "stage_settlements",
        [
            "settlement_id",
            "merchant_id",
            "business_date",
            "settlement_date",
            "gross_amount_minor",
            "refund_amount_minor",
            "fee_amount_minor",
            "tax_amount_minor",
            "net_amount_minor",
        ],
    ),
    (
        "payments",
        "stage_payments",
        [
            "payment_id",
            "order_id",
            "settlement_id",
            "business_date",
            "captured_at",
            "amount_minor",
            "fee_minor",
            "tax_minor",
        ],
    ),
    (
        "refunds",
        "stage_refunds",
        [
            "refund_id",
            "payment_id",
            "refunded_at",
            "amount_minor",
        ],
    ),
    (
        "bank_credits",
        "stage_bank_credits",
        [
            "bank_entry_id",
            "settlement_id",
            "credited_on",
            "amount_minor",
        ],
    ),
]


def database_size(connection):
    return connection.execute(
        "SELECT pg_database_size(current_database());"
    ).fetchone()[0]


def load_and_verify(connection, table, stage, columns, run_id):
    """
    Insert new records, then verify that all expected records exist
    with matching business values.
    """
    identifiers = sql.SQL(", ").join(
        sql.Identifier(column) for column in columns
    )

    target = sql.Identifier("batch", table)
    staging = sql.Identifier(stage)

    insert_query = sql.SQL("""
        INSERT INTO {} ({}, ingestion_run_id)
        SELECT {}, %s
        FROM {}
        ON CONFLICT DO NOTHING;
    """).format(
        target,
        identifiers,
        identifiers,
        staging,
    )

    inserted = connection.execute(
        insert_query, (run_id,)
    ).rowcount

    # Detect missing rows or changed values on reruns.
    # Audit run IDs are intentionally excluded from this comparison.
    verification_query = sql.SQL("""
        SELECT COUNT(*)
        FROM (
            SELECT {} FROM {}
            EXCEPT
            SELECT {} FROM {}
        ) AS differences;
    """).format(
        identifiers,
        staging,
        identifiers,
        target,
    )

    differences = connection.execute(
        verification_query
    ).fetchone()[0]

    if differences:
        raise ValueError(
            f"{table}: {differences} expected records differ "
            "from stored data."
        )

    expected = connection.execute(
        sql.SQL("SELECT COUNT(*) FROM {}").format(staging)
    ).fetchone()[0]

    return expected, inserted


def process_day(connection, batch_date):
    next_date = batch_date + timedelta(days=1)
    source_name = f"{SOURCE_PREFIX}:{batch_date.isoformat()}"
    run_id = uuid.uuid4()

    # Persist attempt before the daily transaction.
    connection.execute(
        """
        INSERT INTO audit.ingestion_runs (
            run_id, dataset_name, source_name, status
        )
        VALUES (%s, %s, %s, 'RUNNING');
        """,
        (run_id, DATASET, source_name),
    )

    try:
        with connection.transaction():

            connection.execute(
                CREATE_DAY_ORDERS,
                (
                    f"SYN_V2_{batch_date:%Y%m%d}_ORD_",
                    f"SYN_V2_{next_date:%Y%m%d}_ORD_",
                ),
            )

            source_count, invalid_dates = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE (
                            order_created_at
                            AT TIME ZONE 'Asia/Kolkata'
                        )::DATE <> %s
                    )
                FROM day_orders;
                """,
                (batch_date,),
            ).fetchone()

            if not 2000 <= source_count <= 3000:
                raise ValueError(
                    f"{batch_date}: expected 2000–3000 source orders, "
                    f"found {source_count}. Complete the orders load first."
                )

            if invalid_dates:
                raise ValueError(
                    f"{batch_date}: source order dates do not match IDs."
                )

            # Fingerprint the source content and simulation version.
            source_digest = connection.execute(
                """
                SELECT MD5(
                    STRING_AGG(
                        ROW(
                            order_id,
                            merchant_id,
                            order_amount_minor,
                            currency,
                            order_status,
                            order_created_at,
                            source_updated_at
                        )::TEXT,
                        '|' ORDER BY order_id
                    )
                )
                FROM day_orders;
                """
            ).fetchone()[0]

            fingerprint = hashlib.sha256(
                f"{DATASET}|{batch_date}|{source_digest}".encode()
            ).hexdigest()

            previous = connection.execute(
                """
                SELECT source_sha256
                FROM audit.ingestion_runs
                WHERE dataset_name = %s
                  AND source_name = %s
                  AND status = 'SUCCESS'
                ORDER BY started_at DESC
                LIMIT 1;
                """,
                (DATASET, source_name),
            ).fetchone()

            if previous and previous[0] != fingerprint:
                raise ValueError(
                    "Source orders changed after a successful load. "
                    "A deliberate correction process is required."
                )

            connection.execute(
                CREATE_PAYMENTS, (batch_date, batch_date)
            )
            connection.execute(CREATE_REFUNDS)
            connection.execute(CREATE_SETTLEMENTS)
            connection.execute(CREATE_BANK_CREDITS)

            totals = {}
            expected_total = 0
            inserted_total = 0

            for table, stage, columns in TABLES:
                expected, inserted = load_and_verify(
                    connection,
                    table,
                    stage,
                    columns,
                    run_id,
                )

                totals[table] = (expected, inserted)
                expected_total += expected
                inserted_total += inserted

            # Here rows_read means generated records across the
            # four output datasets, not the source order count.
            connection.execute(
                """
                UPDATE audit.ingestion_runs
                SET status = 'SUCCESS',
                    source_sha256 = %s,
                    rows_read = %s,
                    rows_inserted = %s,
                    rows_rejected = 0,
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = %s;
                """,
                (
                    fingerprint,
                    expected_total,
                    inserted_total,
                    run_id,
                ),
            )

        return source_count, totals

    except Exception as error:
        safe_error = (
            str(error)
            if isinstance(error, ValueError)
            else type(error).__name__
        )

        try:
            connection.execute(
                """
                UPDATE audit.ingestion_runs
                SET status = 'FAILED',
                    error_message = %s,
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = %s;
                """,
                (safe_error, run_id),
            )
        except psycopg.Error:
            print(
                "Could not record failure; audit may remain RUNNING.",
                flush=True,
            )

        print(f"Failed date: {batch_date}; run ID: {run_id}")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2026, 1, 1),
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date(2026, 9, 30),
    )

    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("Start date must not exceed end date.")

    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is missing from .env.")

    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=30,
        autocommit=True,
        application_name="payrecon_batch_pack",
    ) as connection:

        # Stable timestamp formatting for source fingerprints.
        connection.execute("SET TIME ZONE 'UTC';")

        # Requires the direct Neon connection URL.
        locked = connection.execute(
            "SELECT pg_try_advisory_lock(20261017, 1);"
        ).fetchone()[0]

        if not locked:
            raise SystemExit(
                "Another batch-pack loader is already running."
            )

        batch_date = args.start_date
        completed = 0

        while batch_date <= args.end_date:
            size = database_size(connection)

            if size >= DATABASE_CHECKPOINT_BYTES:
                print(
                    f"STOPPED before {batch_date}: "
                    f"database size is {size / 1_000_000:.1f} MB."
                )
                print(
                    "Earlier completed days remain committed. "
                    "Review Neon storage before continuing."
                )
                raise SystemExit(2)

            source_count, totals = process_day(
                connection, batch_date
            )

            print(
                f"\n{batch_date} | source orders={source_count:,}",
                flush=True,
            )

            for table, (expected, inserted) in totals.items():
                print(
                    f"  {table}: expected={expected:,}, "
                    f"inserted={inserted:,}, "
                    f"existing={expected - inserted:,}",
                    flush=True,
                )

            completed += 1
            batch_date += timedelta(days=1)

        print(f"\nSUCCESS: Completed {completed} requested dates.")
        print(
            f"Database size: "
            f"{database_size(connection) / 1_000_000:.1f} MB"
        )


if __name__ == "__main__":
    try:
        main()
    except psycopg.Error as error:
        print(f"Database error: {type(error).__name__}")
        print(f"SQLSTATE: {error.sqlstate or 'Not available'}")
        raise SystemExit(1)
    except ValueError as error:
        print(f"Validation error: {error}")
        raise SystemExit(1)