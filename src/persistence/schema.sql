-- Deals table: one row per broker listing
CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    source TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    source_url TEXT NOT NULL,

    content_hash TEXT NOT NULL,

    decision TEXT NOT NULL,
    decision_confidence TEXT,
    reasons TEXT,

    extracted_json TEXT NOT NULL,

    first_seen DATE NOT NULL,
    last_seen DATE NOT NULL,
    last_updated TIMESTAMP NOT NULL,

    pdf_path TEXT,

    UNIQUE (source, source_listing_id)
);

-- Daily click tracking (rate limit safety)
CREATE TABLE IF NOT EXISTS daily_clicks (
    date DATE NOT NULL,
    broker TEXT NOT NULL,
    clicks_used INTEGER NOT NULL,

    PRIMARY KEY (date, broker)
);