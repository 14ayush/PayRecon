import argparse
import hashlib
import json
import os
import random
import uuid

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

SOURCE_NAME = "synthetic_orders_v1"
DATASET_NAME = "orders"

# India Standard Time has no daylight-saving changes.
IST = timezone(timedelta(hours=5, minutes=30))

MIN_ORDERS_PER_DAY = 2_000
MAX_ORDERS_PER_DAY = 3_000

# Conservative development threshold, expressed in decimal MB.
DATABASE_CHECKPOINT_BYTES = 350_000_000

COLUMNS = (
    "order_id",
    "merchant_id",
    "order_amount_minor",
    "currency",
    "order_status",
    "order_created_at",
    "source_updated_at",
)


def deterministic_seed(batch_date, purpose):
    """Stable across different runs and computers."""
    text = f"{SOURCE_NAME}|{batch_date.isoformat()}|{purpose}"
    return int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8],
        byteorder="big",
    )


def daily_order_count(batch_date):
    rng = random.Random(deterministic_seed(batch_date, "count"))
    return rng.randint(MIN_ORDERS_PER_DAY, MAX_ORDERS_PER_DAY)


def generate_orders(batch_date):
    """Generate one reproducible day of synthetic merchant orders."""
    rng = random.Random(deterministic_seed(batch_date, "records"))
    day_start = datetime.combine(batch_date, time.min, tzinfo=IST)
    date_tag = batch_date.strftime("%Y%m%d")

    orders = []

    for number in range(1, daily_order_count(batch_date) + 1):
        created_at = day_start + timedelta(
            seconds=rng.randrange(24 * 60 * 60)
        )

        status = rng.choices(
            ["CREATED", "PAID", "CANCELLED"],
            weights=[15, 80, 5],
            k=1,
        )[0]

        updated_at = created_at

        if status != "CREATED":
            updated_at += timedelta(minutes=rng.randint(1, 60))

        orders.append(
            {
                "order_id": (
                    f"SYN_V2_{date_tag}_ORD_{number:06d}"
                ),
                "merchant_id": (
                    f"SYN_MERCHANT_{rng.randint(1, 10):03d}"
                ),
                "order_amount_minor": rng.randint(100, 500_000),
                "currency": "INR",
                "order_status": status,
                "order_created_at": created_at,
                "source_updated_at": updated_at,
            }
        )

    return orders


def count_invalid_records(orders, batch_date):
    seen_ids = set()
    rejected = 0

    for order in orders:
        valid = (
            bool(order["order_id"])
            and bool(order["merchant_id"])
            and order["order_id"] not in seen_ids
            and type(order["order_amount_minor"]) is int
            and order["order_amount_minor"] > 0
            and order["currency"] == "INR"
            and order["order_status"] in {
                "CREATED", "PAID", "CANCELLED"
            }
            and order["order_created_at"].astimezone(IST).date()
                == batch_date
            and order["source_updated_at"]
                >= order["order_created_at"]
        )

        if not valid:
            rejected += 1

        seen_ids.add(order["order_id"])

    return rejected


def dataset_checksum(orders):
    payload = json.dumps(
        orders,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: value.isoformat(),
    ).encode("utf-8")

    return hashlib.sha256(payload).hexdigest()


def database_size_bytes(connection):
    row = connection.execute(
        "SELECT pg_database_size(current_database());"
    ).fetchone()

    return row[0]


def load_one_day(connection, batch_date):
    orders = generate_orders(batch_date)
    rejected = count_invalid_records(orders, batch_date)
    checksum = dataset_checksum(orders)
    run_id = uuid.uuid4()

    # Commit the audit attempt independently of the data transaction.
    connection.execute(
        """
        INSERT INTO audit.ingestion_runs (
            run_id,
            dataset_name,
            source_name,
            source_sha256,
            status,
            rows_read,
            rows_rejected
        )
        VALUES (%s, %s, %s, %s, 'RUNNING', %s, %s);
        """,
        (
            run_id,
            DATASET_NAME,
            f"{SOURCE_NAME}:{batch_date.isoformat()}",
            checksum,
            len(orders),
            rejected,
        ),
    )

    try:
        if rejected:
            raise ValueError(
                f"Validation failed for {rejected} records."
            )

        with connection.transaction():
            with connection.cursor() as cursor:

                # Temporary staging exists only for this transaction.
                cursor.execute(
                    """
                    CREATE TEMP TABLE orders_stage (
                        order_id TEXT,
                        merchant_id TEXT,
                        order_amount_minor BIGINT,
                        currency VARCHAR(3),
                        order_status TEXT,
                        order_created_at TIMESTAMPTZ,
                        source_updated_at TIMESTAMPTZ
                    ) ON COMMIT DROP;
                    """
                )

                # COPY avoids thousands of individual INSERT requests.
                with cursor.copy(
                    """
                    COPY orders_stage (
                        order_id,
                        merchant_id,
                        order_amount_minor,
                        currency,
                        order_status,
                        order_created_at,
                        source_updated_at
                    )
                    FROM STDIN
                    """
                ) as copy:
                    for order in orders:
                        copy.write_row(
                            tuple(order[column] for column in COLUMNS)
                        )

                # Existing records must match this immutable dataset.
                # Detect changed generator output instead of silently
                # skipping conflicting values.
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM orders_stage AS s
                    JOIN batch.orders AS b
                      ON b.order_id = s.order_id
                    WHERE ROW(
                        b.merchant_id,
                        b.order_amount_minor,
                        b.currency,
                        b.order_status,
                        b.order_created_at,
                        b.source_updated_at
                    ) IS DISTINCT FROM ROW(
                        s.merchant_id,
                        s.order_amount_minor,
                        s.currency,
                        s.order_status,
                        s.order_created_at,
                        s.source_updated_at
                    );
                    """
                )

                if cursor.fetchone()[0]:
                    raise ValueError(
                        "Existing IDs have different source values."
                    )

                cursor.execute(
                    """
                    INSERT INTO batch.orders (
                        order_id,
                        merchant_id,
                        order_amount_minor,
                        currency,
                        order_status,
                        order_created_at,
                        source_updated_at,
                        ingestion_run_id
                    )
                    SELECT
                        order_id,
                        merchant_id,
                        order_amount_minor,
                        currency,
                        order_status,
                        order_created_at,
                        source_updated_at,
                        %s
                    FROM orders_stage
                    ON CONFLICT (order_id) DO NOTHING;
                    """,
                    (run_id,),
                )

                inserted = cursor.rowcount
                skipped = len(orders) - inserted

                cursor.execute(
                    """
                    UPDATE audit.ingestion_runs
                    SET status = 'SUCCESS',
                        rows_inserted = %s,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s;
                    """,
                    (inserted, run_id),
                )

        return len(orders), inserted, skipped

    except Exception as error:
        # Data writes have rolled back. Record the failure separately.
        error_category = type(error).__name__

        # Validation messages are generated by this script.
        # Avoid storing arbitrary database errors containing details.
        error_message = (
            str(error)
            if isinstance(error, ValueError)
            else error_category
        )

        try:
            connection.execute(
                """
                UPDATE audit.ingestion_runs
                SET status = 'FAILED',
                    finished_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE run_id = %s;
                """,
                (error_message, run_id),
            )
        except psycopg.Error:
            print(
                "Audit update failed; this run may remain RUNNING.",
                flush=True,
            )

        print(f"Failed date: {batch_date}", flush=True)
        print(f"Run ID: {run_id}", flush=True)
        print(f"Error category: {error_category}", flush=True)
        raise


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Load historical PayRecon synthetic orders."
    )

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
        parser.error("--start-date must not exceed --end-date.")

    return args


def main():
    args = parse_arguments()

    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is missing from .env.")

    number_of_days = (args.end_date - args.start_date).days + 1

    dates = [
        args.start_date + timedelta(days=offset)
        for offset in range(number_of_days)
    ]

    planned_orders = sum(daily_order_count(day) for day in dates)

    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Days: {number_of_days}")
    print(f"Planned source orders: {planned_orders:,}")
    print("Daily volume: 2,500–5,000 orders")
    print("Storage checkpoint: 350 MB")
    print(flush=True)

    total_inserted = 0
    total_skipped = 0

    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=30,
        application_name="payrecon_historical_orders",
        autocommit=True,
    ) as connection:

        for position, batch_date in enumerate(dates, start=1):
            size = database_size_bytes(connection)

            if size >= DATABASE_CHECKPOINT_BYTES:
                print(
                    f"STOPPED before {batch_date}: database size "
                    f"is {size / 1_000_000:.1f} MB.",
                    flush=True,
                )
                print(
                    "Previous days remain committed. Check Neon "
                    "storage before continuing.",
                    flush=True,
                )
                raise SystemExit(2)

            read, inserted, skipped = load_one_day(
                connection, batch_date
            )

            total_inserted += inserted
            total_skipped += skipped

            print(
                f"[{position}/{number_of_days}] {batch_date} | "
                f"read={read:,} | inserted={inserted:,} | "
                f"skipped={skipped:,}",
                flush=True,
            )

        final_size = database_size_bytes(connection)

    print("\nSUCCESS: Requested date range completed.")
    print(f"Inserted in this execution: {total_inserted:,}")
    print(f"Existing orders skipped: {total_skipped:,}")
    print(f"Database size: {final_size / 1_000_000:.1f} MB")


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