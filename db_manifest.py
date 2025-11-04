import os
import time
import tempfile
import hashlib
import shutil
from pathlib import Path
from typing import Dict, Optional
import requests
import psycopg
from psycopg.rows import dict_row

# Load environment variables from .env file
import load_env

DEFAULT_TIMEOUT = (10, 30)  # connect, read
RETRY_STATUSES = {429, 500, 502, 503, 504}

class DBManifest:
    """
    PostgreSQL-based manifest for tracking downloaded files.
    Modes:
      - fast:  skip if (period,url) exists (no network checks)
      - safe:  if exists, do HEAD/conditional-GET; hash+version if content differs
    """
    def __init__(self, source_id: str, file_type: str, mode: str = "safe", program: Optional[str] = None):
        self.source_id = source_id
        self.file_type = file_type  # "monthly" or "annual"
        self.program = program      # "IV" or "NIV" (for monthly only)
        self.mode = mode.lower().strip()  # "fast" | "safe"
        
        # Get database URL from Railway environment
        self.db_url = os.environ.get("DATABASE_URL")
        if not self.db_url:
            raise RuntimeError("DATABASE_URL environment variable not set")
    
    def _get_conn(self):
        """Get a new database connection."""
        return psycopg.connect(self.db_url, row_factory=dict_row)
    
    def _retrying_head(self, session: requests.Session, url: str):
        """HEAD request with retries."""
        backoff = 1.0
        for i in range(4):
            try:
                resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
                if resp.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"retryable {resp.status_code}")
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i == 3: 
                    print(f"HEAD failed for {url}: {e}")
                    raise
                time.sleep(backoff)
                backoff *= 2
    
    def _retrying_get(self, session: requests.Session, url: str, headers=None, stream=True):
        """GET request with retries."""
        backoff = 1.0
        for i in range(4):
            try:
                resp = session.get(url, headers=headers or {}, timeout=DEFAULT_TIMEOUT, stream=stream)
                if resp.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"retryable {resp.status_code}")
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i == 3:
                    print(f"GET failed for {url}: {e}")
                    raise
                time.sleep(backoff)
                backoff *= 2
    
    def get_existing(self, period: str, url: str) -> Optional[Dict]:
        """Get existing manifest entry for this period/url."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM file_manifest 
                    WHERE period = %s AND url = %s AND status = 'active'
                    ORDER BY version DESC
                    LIMIT 1
                """, (period, url))
                return cur.fetchone()
    
    def plan(self, period: str, url: str) -> Dict[str, str]:
        """
        Decide whether to skip, download, or version this file.
        Returns: {"decision": "skip"|"download"|"version", "reason": str}
        """
        existing = self.get_existing(period, url)
        
        if not existing:
            return {"decision": "download", "reason": "unseen"}
        
        if self.mode == "fast":
            return {"decision": "skip", "reason": "seen-fast"}
        
        # Safe mode: check if content changed
        etag_prev = existing.get("etag") or None
        lm_prev = existing.get("last_modified") or None
        size_prev = existing.get("bytes")
        
        try:
            with requests.Session() as s:
                h = self._retrying_head(s, url)
            
            etag = h.headers.get("ETag")
            lm = h.headers.get("Last-Modified")
            size = h.headers.get("Content-Length")
            try:
                size = int(size) if size is not None else None
            except:
                size = None
            
            # Check ETag match
            if etag_prev and etag and etag_prev == etag:
                return {"decision": "skip", "reason": "etag-match"}
            
            # Check Last-Modified match
            if lm_prev and lm and lm_prev == lm and (size_prev == size or size is None):
                return {"decision": "skip", "reason": "last-modified-match"}
            
            # Try conditional GET
            headers = {}
            if etag_prev:
                headers["If-None-Match"] = etag_prev
            if lm_prev:
                headers["If-Modified-Since"] = lm_prev
            
            with requests.Session() as s:
                r = self._retrying_get(s, url, headers=headers, stream=True)
                if r.status_code == 304:
                    return {"decision": "skip", "reason": "not-modified"}
            
            return {"decision": "version", "reason": "content-may-differ"}
        
        except Exception:
            # Be conservative: assume it may have changed
            return {"decision": "version", "reason": "head-failed"}
    
    def _stream_to_temp_and_hash(self, resp, tmpdir: Path):
        """Download response to temp file and calculate hash."""
        tmpdir.mkdir(parents=True, exist_ok=True)
        fd, tmppath = tempfile.mkstemp(prefix="dl_", dir=tmpdir)
        h = hashlib.sha256()
        size = 0
        
        with os.fdopen(fd, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                h.update(chunk)
                f.write(chunk)
                size += len(chunk)
        
        return Path(tmppath), h.hexdigest(), size
    
    def _get_next_version(self, period: str, url: str) -> int:
        """Get the next version number for this period/url."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(version) as max_ver 
                    FROM file_manifest 
                    WHERE period = %s AND url = %s
                """, (period, url))
                result = cur.fetchone()
                max_ver = result.get("max_ver") if result else None
                return (max_ver or 0) + 1
    
    def download_and_record(
        self, 
        session: requests.Session, 
        url: str, 
        outdir: str, 
        period: str, 
        versioned: bool
    ) -> Optional[str]:
        """
        Download file and record in manifest.
        Returns file path if downloaded, None if unchanged.
        """
        Path(outdir).mkdir(parents=True, exist_ok=True)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        existing = self.get_existing(period, url)
        headers = {}
        
        if self.mode == "safe" and existing:
            if existing.get("etag"):
                headers["If-None-Match"] = existing["etag"]
            if existing.get("last_modified"):
                headers["If-Modified-Since"] = existing["last_modified"]
        
        r = self._retrying_get(session, url, headers=headers, stream=True)
        
        if r.status_code == 304:
            return None
        
        # Download to temp and hash
        tmppath, sha256, size = self._stream_to_temp_and_hash(r, Path(outdir) / ".tmp")
        
        # Check if hash matches existing
        if self.mode == "safe" and existing and existing.get("sha256") == sha256:
            tmppath.unlink(missing_ok=True)
            return None
        
        # Determine filename
        url_name = url.split("?")[0].rstrip("/").split("/")[-1] or f"{period}.bin"
        ver = 1
        
        if versioned:
            ver = self._get_next_version(period, url)
            base, dot, ext = url_name.partition(".")
            url_name = f"{base}.v{ver}.{ext}" if dot else f"{base}.v{ver}"
        
        final_path = str((Path(outdir) / url_name).resolve())
        shutil.move(str(tmppath), final_path)
        
        # Mark previous version as replaced
        if versioned and existing:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE file_manifest 
                        SET status = 'replaced', updated_at = NOW()
                        WHERE period = %s AND url = %s AND status = 'active'
                    """, (period, url))
                    conn.commit()
        
        # Insert new record
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO file_manifest (
                        source_id, file_type, program, period, url, 
                        filename, saved_path, bytes, sha256, 
                        etag, last_modified, version, 
                        status, downloaded_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        'active', %s
                    )
                """, (
                    self.source_id,
                    self.file_type,
                    self.program,
                    period,
                    url,
                    os.path.basename(final_path),
                    final_path,
                    size,
                    sha256,
                    r.headers.get("ETag") or "",
                    r.headers.get("Last-Modified") or "",
                    ver,
                    now
                ))
                conn.commit()
        
        return final_path
    
    def register_existing_file(self, period: str, url: str, file_path: str) -> bool:
        """
        Register an existing file in the manifest without downloading.
        Returns True if registered, False if already in manifest or file doesn't exist.
        """
        # Check if already in manifest
        existing = self.get_existing(period, url)
        if existing:
            return False
        
        file_path = Path(file_path)
        if not file_path.exists():
            return False
        
        # Calculate hash and size
        h = hashlib.sha256()
        size = 0
        with file_path.open("rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
                size += len(chunk)
        
        sha256 = h.hexdigest()
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        # Insert record
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO file_manifest (
                        source_id, file_type, program, period, url,
                        filename, saved_path, bytes, sha256,
                        etag, last_modified, version,
                        status, downloaded_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        '', '', 1,
                        'active', %s
                    )
                """, (
                    self.source_id,
                    self.file_type,
                    self.program,
                    period,
                    url,
                    file_path.name,
                    str(file_path.resolve()),
                    size,
                    sha256,
                    now
                ))
                conn.commit()
        
        print(f"Registered existing file: {period} | {url} -> {file_path}")
        return True
    
    def get_all_active_files(self) -> list[Dict]:
        """Get all active files for this file_type from manifest."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM file_manifest 
                    WHERE file_type = %s AND status = 'active'
                    ORDER BY period, url
                """, (self.file_type,))
                return cur.fetchall()
