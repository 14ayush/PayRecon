import os 
from pathlib import Path
import psycopg
from dotenv import load_dotenv


def main():
    project_root=Path(__file__).resolve().parent
    load_dotenv(project_root/".env")

    database_url=os.getenv("DATABASE_URL")

    if not database_url:
        raise SystemExit("Database url is missing in the .env file")


    try:
        with psycopg.connect(
            database_url,
            connect_timeout=30,
            application_name="payrecon_setup",
            autocommit=True
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT
                        current_database(),
                        pg_size_pretty(
                            pg_database_size(current_database())
                        );
                    """
                )
                database_name,database_size=cursor.fetchone()
                print("SUCCESS: Connected to Neon.")
                print(f"Database: {database_name}")
                print(f"Current database size: {database_size}")
    
    except psycopg.Error as error:
        message = str(error)

        # Hide the complete connection URL if it appears in the error.
        if database_url:
            message = message.replace(database_url, "[REDACTED_URL]")

        print(f"Connection failed: {type(error).__name__}")
        print(f"Details: {message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()