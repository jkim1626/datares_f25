import os
import shutil
import psycopg
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def reset_database():
    """Drop all tables and views in the connected PostgreSQL database."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")

    print("🗑️  Resetting PostgreSQL database...")

    with psycopg.connect(db_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Drop all views
            cur.execute("""
                DO $$
                DECLARE
                    rec RECORD;
                BEGIN
                    FOR rec IN (SELECT table_schema, table_name
                                FROM information_schema.views
                                WHERE table_schema = 'public')
                    LOOP
                        EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE;',
                                       rec.table_schema, rec.table_name);
                    END LOOP;
                END $$;
            """)
            # Drop all tables
            cur.execute("""
                DO $$
                DECLARE
                    rec RECORD;
                BEGIN
                    FOR rec IN (SELECT table_schema, table_name
                                FROM information_schema.tables
                                WHERE table_schema = 'public' AND table_type='BASE TABLE')
                    LOOP
                        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;',
                                       rec.table_schema, rec.table_name);
                    END LOOP;
                END $$;
            """)
    print("✅ Database fully dropped.\n")


def reset_volume():
    """Delete all subfolders under /data to clear volume state."""
    data_root = Path(os.environ.get("DATA_ROOT", "/data"))
    print(f"🗑️  Clearing volume folders under {data_root}...")

    if not data_root.exists():
        print(f"⚠️ Volume root {data_root} does not exist, skipping.")
        return

    for subdir in data_root.iterdir():
        if subdir.is_dir():
            print(f"   Removing {subdir}")
            shutil.rmtree(subdir, ignore_errors=True)
    print("✅ Volume cleared.\n")


if __name__ == "__main__":
    print("============================================================")
    print("⚙️  HARD RESET: Dropping database tables/views and clearing /data volume")
    print("============================================================\n")
    try:
        reset_database()
    except Exception as e:
        print(f"⚠️ Database reset failed or partial: {e}")

    try:
        reset_volume()
    except Exception as e:
        print(f"⚠️ Volume cleanup failed or partial: {e}")

    print("🎯 Reset complete. Ready to reinitialize database and rerun tests.")
