import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

def extract_drive_file_id(url: str) -> str | None:
    m = re.search(r"/d/([^/]+)/", url)
    return m.group(1) if m else None

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    """
    SELECT
        id,
        source,
        pdf_drive_url,
        industry,
        sector
    FROM deals
    WHERE pdf_drive_url IS NOT NULL
    """
).fetchall()

for r in rows:
    drive_file_id = extract_drive_file_id(r["pdf_drive_url"])

    conn.execute(
        """
        INSERT INTO deal_artifacts (
            deal_id,
            broker,
            artifact_type,
            artifact_name,
            drive_file_id,
            drive_url,
            industry_at_create,
            sector_at_create,
            created_by
        )
        VALUES (?, ?, 'pdf', NULL, ?, ?, ?, ?, 'backfill_from_deals')
        """,
        (
            r["id"],
            r["source"],
            drive_file_id,
            r["pdf_drive_url"],
            r["industry"],
            r["sector"],
        ),
    )

conn.commit()