import sqlite3
from pathlib import Path
from datetime import date

from src.brokers.knightsbridge_client import KnightsbridgeClient
from src.sector_mappings.knightsbridge import (
    resolve_knightsbridge_sector
)
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
from src.config import KB_USERNAME, KB_PASSWORD

if not KB_USERNAME or not KB_PASSWORD:
    print("⏭️ Skipping Knightsbridge import (credentials not set)")
    exit(0)

def normalize_knightsbridge_sector(s: str | None) -> str | None:
    if not s:
        return None
    return (
        s.replace("\xa0", " ")
         .replace("&amp;", "&")
         .strip()
    )

def import_knightsbridge():
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    client = KnightsbridgeClient()
    client.start()

    try:
        rows = client.fetch_index()
        today = date.today().isoformat()

        inserted = 0
        refreshed = 0

        for row in rows:
            cur = conn.execute(
                """
                SELECT id
                FROM deals
                WHERE source = ?
                  AND source_listing_id = ?
                """,
                (row["source"], row["source_listing_id"]),
            ).fetchone()

            if cur:
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        last_seen = ?,
                        title = COALESCE(title, ?),
                        source_url = ?
                    WHERE id = ?
                    """,
                    (
                        today,
                        row["title"],
                        row["source_url"],
                        cur["id"],
                    ),
                )
                refreshed += 1
                continue
            normalized_sector_raw = normalize_knightsbridge_sector(
                row["sector_raw"]
            )

            if normalized_sector_raw is None:
                industry = "Other"
                sector = None
                sector_source = "inferred"
                confidence = 0.3
                reason = "broker_missing_sector"
            else:
                (
                    industry,
                    sector,
                    sector_source,
                    confidence,
                    reason,
                ) = resolve_knightsbridge_sector(normalized_sector_raw)

            conn.execute(

                """

                INSERT INTO deals (source,
                                   source_listing_id,
                                   source_url,
                                   title,
                                   sector_raw,
                                   industry,
                                   sector,
                                   sector_source,
                                   sector_inference_confidence,
                                   sector_inference_reason,
                                   first_seen,
                                   last_seen,
                                   needs_detail_refresh)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    row["source"],
                    row["source_listing_id"],
                    row["source_url"],
                    row["title"],
                    normalized_sector_raw,
                    industry,
                    sector,
                    sector_source,
                    confidence,
                    reason,
                    today,
                    today,
                ),
            )
            inserted += 1

        conn.commit()

        print(
            f"✅ Knightsbridge import complete — "
            f"inserted={inserted}, refreshed={refreshed}"
        )

    finally:
        client.stop()
        conn.close()

if __name__ == "__main__":
    import_knightsbridge()