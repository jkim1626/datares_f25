#!/usr/bin/env python3
"""
Example queries for the file_manifest database.
Demonstrates common use cases for querying downloaded files.
"""

import os
import psycopg
from psycopg.rows import dict_row

# Load environment variables from .env file
import load_env

def get_connection():
    """Get database connection."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(db_url, row_factory=dict_row)


def example_1_recent_downloads():
    """Get the most recent 10 downloads."""
    print("\n" + "=" * 60)
    print("Example 1: Recent Downloads")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    file_type,
                    program,
                    period,
                    filename,
                    downloaded_at
                FROM file_manifest
                WHERE status = 'active'
                ORDER BY downloaded_at DESC
                LIMIT 10
            """)
            
            results = cur.fetchall()
            for r in results:
                prog = r['program'] if r['program'] else 'N/A'
                print(f"  {r['downloaded_at']} | {r['file_type']:8} | {prog:4} | {r['period']:12} | {r['filename']}")


def example_2_files_by_period():
    """Get all files for a specific period."""
    print("\n" + "=" * 60)
    print("Example 2: Files for Specific Period")
    print("=" * 60)
    
    period = "FY2024-10"  # Change this to your desired period
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    file_type,
                    program,
                    filename,
                    saved_path,
                    bytes
                FROM file_manifest
                WHERE period = %s AND status = 'active'
                ORDER BY file_type, program
            """, (period,))
            
            results = cur.fetchall()
            print(f"\nFound {len(results)} files for period {period}:\n")
            
            for r in results:
                prog = r['program'] if r['program'] else 'N/A'
                size_mb = r['bytes'] / (1024 * 1024) if r['bytes'] else 0
                print(f"  {r['file_type']:8} | {prog:4} | {size_mb:6.2f} MB | {r['filename']}")


def example_3_storage_by_type():
    """Calculate storage used by file type."""
    print("\n" + "=" * 60)
    print("Example 3: Storage Usage by Type")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    file_type,
                    program,
                    COUNT(*) as file_count,
                    SUM(bytes) as total_bytes,
                    AVG(bytes) as avg_bytes
                FROM file_manifest
                WHERE status = 'active'
                GROUP BY file_type, program
                ORDER BY file_type, program
            """)
            
            results = cur.fetchall()
            print(f"\n{'Type':<10} {'Program':<8} {'Files':<8} {'Total Size':<15} {'Avg Size':<12}")
            print("-" * 60)
            
            for r in results:
                prog = r['program'] if r['program'] else 'N/A'
                total_mb = r['total_bytes'] / (1024 * 1024)
                avg_mb = r['avg_bytes'] / (1024 * 1024)
                print(f"{r['file_type']:<10} {prog:<8} {r['file_count']:<8} {total_mb:>10.2f} MB   {avg_mb:>8.2f} MB")


def example_4_missing_files():
    """Find files that are missing from disk."""
    print("\n" + "=" * 60)
    print("Example 4: Missing Files")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    file_type,
                    program,
                    period,
                    filename,
                    saved_path,
                    updated_at
                FROM file_manifest
                WHERE status = 'missing'
                ORDER BY updated_at DESC
                LIMIT 20
            """)
            
            results = cur.fetchall()
            
            if results:
                print(f"\nFound {len(results)} missing files:\n")
                for r in results:
                    prog = r['program'] if r['program'] else 'N/A'
                    print(f"  {r['file_type']:8} | {prog:4} | {r['period']:12} | {r['filename']}")
                    print(f"    Path: {r['saved_path']}")
                    print(f"    Marked missing: {r['updated_at']}\n")
            else:
                print("\n✅ No missing files!")


def example_5_download_history():
    """Show download activity over time."""
    print("\n" + "=" * 60)
    print("Example 5: Download Activity (Last 30 Days)")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(downloaded_at) as download_date,
                    file_type,
                    COUNT(*) as downloads
                FROM file_manifest
                WHERE downloaded_at > NOW() - INTERVAL '30 days'
                GROUP BY DATE(downloaded_at), file_type
                ORDER BY download_date DESC, file_type
            """)
            
            results = cur.fetchall()
            
            if results:
                print(f"\n{'Date':<12} {'Type':<10} {'Downloads':<10}")
                print("-" * 40)
                for r in results:
                    print(f"{r['download_date']} {r['file_type']:<10} {r['downloads']:<10}")
            else:
                print("\nNo downloads in the last 30 days")


def example_6_file_versions():
    """Find files that have multiple versions."""
    print("\n" + "=" * 60)
    print("Example 6: Files with Multiple Versions")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    period,
                    url,
                    COUNT(*) as version_count,
                    MAX(version) as latest_version,
                    MAX(downloaded_at) as last_updated
                FROM file_manifest
                GROUP BY period, url
                HAVING COUNT(*) > 1
                ORDER BY last_updated DESC
                LIMIT 10
            """)
            
            results = cur.fetchall()
            
            if results:
                print(f"\nFound {len(results)} files with multiple versions:\n")
                for r in results:
                    print(f"  Period: {r['period']}")
                    print(f"  Versions: {r['version_count']} (latest: v{r['latest_version']})")
                    print(f"  Last updated: {r['last_updated']}")
                    print(f"  URL: {r['url']}\n")
            else:
                print("\nNo files with multiple versions")


def example_7_find_specific_file():
    """Search for files by name pattern."""
    print("\n" + "=" * 60)
    print("Example 7: Search Files by Name")
    print("=" * 60)
    
    search_term = "2024"  # Change this to search for different terms
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    file_type,
                    program,
                    period,
                    filename,
                    saved_path,
                    bytes
                FROM file_manifest
                WHERE filename ILIKE %s AND status = 'active'
                ORDER BY downloaded_at DESC
                LIMIT 20
            """, (f"%{search_term}%",))
            
            results = cur.fetchall()
            
            print(f"\nSearching for files matching '{search_term}'...\n")
            
            if results:
                print(f"Found {len(results)} files:\n")
                for r in results:
                    prog = r['program'] if r['program'] else 'N/A'
                    size_mb = r['bytes'] / (1024 * 1024) if r['bytes'] else 0
                    print(f"  {r['file_type']:8} | {prog:4} | {r['period']:12} | {size_mb:6.2f} MB | {r['filename']}")
            else:
                print(f"No files found matching '{search_term}'")


def example_8_summary_stats():
    """Get overall summary statistics."""
    print("\n" + "=" * 60)
    print("Example 8: Summary Statistics")
    print("=" * 60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Total files and storage
            cur.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    SUM(bytes) as total_bytes
                FROM file_manifest
                GROUP BY status
            """)
            
            results = cur.fetchall()
            
            print("\nBy Status:")
            print(f"{'Status':<12} {'Count':<10} {'Total Size':<15}")
            print("-" * 40)
            
            total_files = 0
            total_storage = 0
            
            for r in results:
                size_gb = r['total_bytes'] / (1024 * 1024 * 1024) if r['total_bytes'] else 0
                print(f"{r['status']:<12} {r['count']:<10} {size_gb:>10.2f} GB")
                total_files += r['count']
                total_storage += r['total_bytes'] if r['total_bytes'] else 0
            
            print("-" * 40)
            total_gb = total_storage / (1024 * 1024 * 1024)
            print(f"{'TOTAL':<12} {total_files:<10} {total_gb:>10.2f} GB")
            
            # Date range
            cur.execute("""
                SELECT 
                    MIN(downloaded_at) as first_download,
                    MAX(downloaded_at) as last_download
                FROM file_manifest
            """)
            
            date_range = cur.fetchone()
            if date_range['first_download']:
                print(f"\nDate Range:")
                print(f"  First download: {date_range['first_download']}")
                print(f"  Last download:  {date_range['last_download']}")


def main():
    """Run all example queries."""
    print("\n" + "=" * 60)
    print("FILE MANIFEST DATABASE - EXAMPLE QUERIES")
    print("=" * 60)
    
    try:
        # Check connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as count FROM file_manifest")
                count = cur.fetchone()['count']
                print(f"\n✅ Connected to database ({count} total records)\n")
        
        # Run examples
        example_1_recent_downloads()
        example_2_files_by_period()
        example_3_storage_by_type()
        example_4_missing_files()
        example_5_download_history()
        example_6_file_versions()
        example_7_find_specific_file()
        example_8_summary_stats()
        
        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)
        print("\nTip: Modify the functions above to customize queries for your needs.")
        print("Run with: railway run python example_queries.py")
        print()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("1. DATABASE_URL environment variable is set")
        print("2. Database schema has been initialized (run init_db.py)")
        print("3. You're running this with: railway run python example_queries.py")


if __name__ == "__main__":
    main()
