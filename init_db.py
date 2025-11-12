import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def init_database():
    """Initialize database schema from db_schema.sql"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    
    print("Reading schema from db_schema.sql...")
    
    # Read schema file
    with open("db_schema.sql", "r") as f:
        schema_sql = f.read()
    
    print("Connecting to database...")
    
    # Execute schema
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
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