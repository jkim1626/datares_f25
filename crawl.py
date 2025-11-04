import logging
import os
from pathlib import Path
import psycopg
from psycopg.rows import dict_row

# Load environment variables from .env file
import load_env

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection from Railway environment."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return psycopg.connect(db_url, row_factory=dict_row)


def scan_volume_files(base_path: Path):
    """
    Scan the volume and return a dict of {saved_path: file_info}.
    """
    files_on_disk = {}
    
    if not base_path.exists():
        logger.warning(f"Base path does not exist: {base_path}")
        return files_on_disk
    
    # Scan for all files (pdf, xlsx, xls, csv)
    for ext in ["*.pdf", "*.xlsx", "*.xls", "*.csv"]:
        for file_path in base_path.rglob(ext):
            # Skip temp directories
            if ".tmp" in str(file_path):
                continue
            
            files_on_disk[str(file_path.resolve())] = {
                "path": file_path,
                "size": file_path.stat().st_size,
                "exists": True
            }
    
    return files_on_disk


def reconcile_manifest():
    """
    Reconcile the Postgres manifest with files on disk:
    1. Mark files in manifest but missing on disk as 'missing'
    2. Keep track of files on disk not in manifest (info only)
    """
    conn = get_db_connection()
    
    # Get all active files from manifest
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, saved_path, period, url, file_type, program
            FROM file_manifest
            WHERE status = 'active'
        """)
        manifest_files = cur.fetchall()
    
    logger.info(f"Found {len(manifest_files)} active files in manifest")
    
    # Scan volume for actual files
    base_path = Path(os.environ.get("DATA_ROOT", "/data")) / "visa_stats"
    files_on_disk = scan_volume_files(base_path)
    
    logger.info(f"Found {len(files_on_disk)} files on disk")
    
    # Check manifest entries against disk
    missing_count = 0
    found_count = 0
    
    for record in manifest_files:
        saved_path = record["saved_path"]
        
        if saved_path not in files_on_disk:
            # File in manifest but not on disk - mark as missing
            missing_count += 1
            logger.warning(
                f"Missing file: {record['file_type']} | {record.get('program', 'N/A')} | "
                f"{record['period']} | {saved_path}"
            )
            
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE file_manifest
                    SET status = 'missing', updated_at = NOW()
                    WHERE id = %s
                """, (record["id"],))
        else:
            found_count += 1
    
    conn.commit()
    
    # Report files on disk not in manifest (info only - don't auto-add)
    manifest_paths = {r["saved_path"] for r in manifest_files}
    untracked_files = []
    
    for disk_path in files_on_disk.keys():
        if disk_path not in manifest_paths:
            untracked_files.append(disk_path)
    
    if untracked_files:
        logger.info(f"Found {len(untracked_files)} files on disk not in manifest:")
        for path in untracked_files[:10]:  # Show first 10
            logger.info(f"  Untracked: {path}")
        if len(untracked_files) > 10:
            logger.info(f"  ... and {len(untracked_files) - 10} more")
    
    # Summary
    logger.info(
        f"Reconciliation complete: "
        f"found={found_count}, marked_missing={missing_count}, "
        f"untracked_on_disk={len(untracked_files)}"
    )
    
    conn.close()


def main():
    logger.info("Starting manifest reconciliation...")
    
    try:
        reconcile_manifest()
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        raise
    
    logger.info("Manifest reconciliation complete")


if __name__ == "__main__":
    main()
