import os
import psycopg
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def init_database():
    """Initialize database schema from db_schema.sql"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    
    print("Reading schema from helpers/db_schema.sql...")
    
    # Get path relative to this file's location
    schema_path = Path(__file__).parent / "db_schema.sql"
    
    # Read schema file
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    print("Connecting to database...")
    
    # Execute schema
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Drop old views first (they may have different columns)
            print("Dropping old views if they exist...")
            cur.execute("DROP VIEW IF EXISTS active_files_summary CASCADE;")
            cur.execute("DROP VIEW IF EXISTS missing_files CASCADE;")
            cur.execute("DROP VIEW IF EXISTS recent_downloads CASCADE;")
            
            print("Executing schema...")
            cur.execute(schema_sql)
        conn.commit()
    
    print("✅ Database schema initialized successfully")
    print("\nCreated:")
    print("  - file_manifest table")
    print("  - active_files_summary view")
    print("  - missing_files view")
    print("  - recent_downloads view")
    print("\nSupported file_types:")
    print("  - monthly, annual, dol, yearbook, uscis")

if __name__ == "__main__":
    init_database()