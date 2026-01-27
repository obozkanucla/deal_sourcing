# src/persistence/schema_guard.py

REQUIRED_COLUMNS = {
    "sector_raw",
    "sector_source",
    "canonical_external_id",
    "drive_folder_id",
}

def assert_deals_schema(conn):
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(deals)")
    }
    missing = REQUIRED_COLUMNS - cols
    if missing:
        raise RuntimeError(
            f"INVALID DB SCHEMA — missing columns in deals: {sorted(missing)}"
        )