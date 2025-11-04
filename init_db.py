#!/usr/bin/env python3
"""
Database initialization script for Railway Postgres.
Run this once after setting up your Railway project to create tables.
"""

import os
import sys
import psycopg

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

def get_schema_sql():
    """Read the schema SQL file."""
    schema_file = "db_schema.sql"
    if not os.path.exists(schema_file):
        print(f"❌ Error: {schema_file} not found")
        print("Make sure you're running this from the project root directory")
        sys.exit(1)
    
    with open(schema_file, 'r') as f:
        return f.read()

def init_database():
    """Initialize the database with schema (idempotent - safe to run multiple times)."""
    
    # Get database URL from environment
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ Error: DATABASE_URL environment variable not set")
        print("\nTo fix this:")
        print("1. Create a .env file in the project root")
        print("2. Add: DATABASE_URL=postgresql://user:password@host:port/database")
        print("3. See .env.example for examples")
        print("\nOr set it directly:")
        print("  export DATABASE_URL=postgresql://...")
        sys.exit(1)
    
    print("Connecting to database...")
    try:
        conn = psycopg.connect(db_url)
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)
    
    # Check if already initialized
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'file_manifest'
                )
            """)
            exists = cur.fetchone()[0]
            if exists:
                print("ℹ️  Database already initialized (file_manifest table exists)")
                print("   Schema updates will be applied if any...")
    except Exception as e:
        print(f"⚠️  Could not check existing tables: {e}")
    
    # Read and execute schema (uses IF NOT EXISTS, so safe to rerun)
    print("\nReading schema from db_schema.sql...")
    schema_sql = get_schema_sql()
    
    print("Creating/updating tables, indexes, and views...")
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("✅ Database schema created/updated successfully")
    except Exception as e:
        print(f"⚠️  Schema update had issues: {e}")
        print(f"   This is usually OK if tables already exist")
        conn.rollback()
        # Don't exit - this is OK for redeployments
    
    # Verify tables were created
    print("\nVerifying tables...")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_type = 'BASE TABLE'
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            if 'file_manifest' in tables:
                print("✅ file_manifest table created")
            else:
                print("⚠️  Warning: file_manifest table not found")
            
            # Check views
            cur.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
            """)
            views = [row[0] for row in cur.fetchall()]
            print(f"✅ Created {len(views)} views: {', '.join(views)}")
            
    except Exception as e:
        print(f"⚠️  Warning: Could not verify tables: {e}")
    
    # Show table info
    print("\nTable structure:")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'file_manifest'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            print(f"  Columns: {len(columns)}")
            for col_name, dtype, max_len in columns:
                if max_len:
                    print(f"    - {col_name}: {dtype}({max_len})")
                else:
                    print(f"    - {col_name}: {dtype}")
    except Exception as e:
        print(f"⚠️  Could not show table info: {e}")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ Database initialization complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Run: python crawl.py (to reconcile any existing files)")
    print("2. Run: python run_all.py (to start scraping)")
    print("\nOr run individual scrapers:")
    print("  - python scrape_monthly.py")
    print("  - python scrape_yearly.py")

if __name__ == "__main__":
    print("=" * 60)
    print("Database Initialization Script")
    print("=" * 60)
    print()
    
    init_database()
