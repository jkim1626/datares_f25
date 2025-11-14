-- File manifest table for tracking all downloaded files across all data sources
CREATE TABLE IF NOT EXISTS file_manifest (
    id SERIAL PRIMARY KEY,
    
    -- Identification
    source_id VARCHAR(100) NOT NULL,           -- e.g., "visastats", "dolstats", "dhsyearbook", "uscis"
    file_type VARCHAR(20) NOT NULL,            -- "monthly", "annual", "dol", "yearbook", "uscis"
    program VARCHAR(100),                       -- "IV", "NIV", "PERM Program", "h1b", etc. (NULL if not applicable)
    period VARCHAR(100) NOT NULL,              -- "FY2024-10", "2024", "PERM/2024", "h1b/2024", etc.
    
    -- Source info
    url TEXT NOT NULL,                         -- Original download URL
    filename VARCHAR(500) NOT NULL,            -- e.g., "report.pdf"
    saved_path TEXT NOT NULL,                  -- Full path in volume: /data/...
    
    -- File metadata
    bytes BIGINT,                              -- File size in bytes
    sha256 VARCHAR(64),                        -- Content hash for change detection
    etag VARCHAR(255),                         -- HTTP ETag header
    last_modified VARCHAR(255),                -- HTTP Last-Modified header
    
    -- Versioning
    version INTEGER DEFAULT 1,                 -- Version number for updated files
    
    -- Status tracking
    status VARCHAR(20) DEFAULT 'active',       -- 'active', 'missing', 'replaced', 'failed'
    error_message TEXT,                        -- Error details if download failed
    
    -- Timestamps
    downloaded_at TIMESTAMP,                   -- When file was downloaded
    created_at TIMESTAMP DEFAULT NOW(),        -- When record was created
    updated_at TIMESTAMP DEFAULT NOW(),        -- When record was last updated
    
    -- Constraints
    UNIQUE(period, url, version),              -- Prevent duplicate downloads
    CHECK (file_type IN ('monthly', 'annual', 'dol', 'yearbook', 'uscis')),
    CHECK (status IN ('active', 'missing', 'replaced', 'failed'))
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_file_manifest_period ON file_manifest(period);
CREATE INDEX IF NOT EXISTS idx_file_manifest_file_type ON file_manifest(file_type);
CREATE INDEX IF NOT EXISTS idx_file_manifest_status ON file_manifest(status);
CREATE INDEX IF NOT EXISTS idx_file_manifest_program ON file_manifest(program);
CREATE INDEX IF NOT EXISTS idx_file_manifest_url ON file_manifest(url);
CREATE INDEX IF NOT EXISTS idx_file_manifest_saved_path ON file_manifest(saved_path);
CREATE INDEX IF NOT EXISTS idx_file_manifest_source_id ON file_manifest(source_id);

-- Trigger to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_file_manifest_updated_at ON file_manifest;
CREATE TRIGGER update_file_manifest_updated_at 
    BEFORE UPDATE ON file_manifest 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Useful queries for monitoring

-- View active files by type and source
CREATE OR REPLACE VIEW active_files_summary AS
SELECT 
    source_id,
    file_type,
    program,
    COUNT(*) as file_count,
    SUM(bytes) as total_bytes,
    MIN(downloaded_at) as oldest_download,
    MAX(downloaded_at) as newest_download
FROM file_manifest
WHERE status = 'active'
GROUP BY source_id, file_type, program
ORDER BY source_id, file_type, program;

-- View missing files
CREATE OR REPLACE VIEW missing_files AS
SELECT 
    id,
    source_id,
    file_type,
    program,
    period,
    filename,
    saved_path,
    downloaded_at,
    updated_at
FROM file_manifest
WHERE status = 'missing'
ORDER BY updated_at DESC;

-- View recent downloads
CREATE OR REPLACE VIEW recent_downloads AS
SELECT 
    source_id,
    file_type,
    program,
    period,
    filename,
    bytes,
    downloaded_at,
    status
FROM file_manifest
ORDER BY downloaded_at DESC
LIMIT 100;