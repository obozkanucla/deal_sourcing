-- =========================================================
-- DEALS (index + detail lifecycle)
-- =========================================================

CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity
    source TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    source_url TEXT NOT NULL,

    -- Index-level metadata
    sector TEXT,

    -- Detail-level analysis
    content_hash TEXT,
    decision TEXT,
    decision_confidence REAL,
    reasons TEXT,
    extracted_json TEXT,

    -- Lifecycle tracking
    first_seen DATE,
    last_seen DATE,
    last_updated DATETIME,

    -- Artifacts
    pdf_path TEXT,

    UNIQUE (source, source_listing_id)
);

-- =========================================================
-- DAILY CLICK BUDGET TRACKING
-- =========================================================

CREATE TABLE IF NOT EXISTS daily_clicks (
    date DATE NOT NULL,
    broker TEXT NOT NULL,
    clicks_used INTEGER NOT NULL,

    PRIMARY KEY (date, broker)
);