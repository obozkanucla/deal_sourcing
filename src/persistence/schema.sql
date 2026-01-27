-- =========================================================
-- DEALS (index + detail lifecycle)
-- =========================================================

CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    source TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    source_url TEXT,

    title TEXT,
    industry TEXT,
    sector TEXT,
    sector_raw TEXT,
    sector_source TEXT,
    sector_inference_confidence REAL,
    sector_inference_reason TEXT,

    location TEXT,
    location_raw TEXT,

    revenue_k REAL,
    ebitda_k REAL,
    asking_price_k REAL,

    revenue_k_effective REAL,
    ebitda_k_effective REAL,
    asking_price_k_effective REAL,

    revenue_k_manual REAL,
    ebitda_k_manual REAL,
    asking_price_k_manual REAL,

    content_hash TEXT,
    description TEXT,
    description_hash TEXT,
    extracted_json TEXT,

    drive_folder_id TEXT,
    drive_folder_url TEXT,
    pdf_drive_url TEXT,
    pdf_generated_at DATETIME,
    pdf_error TEXT,

    needs_detail_refresh INTEGER DEFAULT 1,
    detail_fetched_at DATETIME,
    detail_fetch_reason TEXT,
    lost_reason TEXT,

    canonical_external_id TEXT,
    broker_name TEXT,
    broker_listing_url TEXT,
    source_role TEXT DEFAULT 'PRIMARY',

    first_seen DATETIME,
    last_seen DATETIME,
    last_updated DATETIME,
    last_updated_source TEXT,

    attributes TEXT,

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