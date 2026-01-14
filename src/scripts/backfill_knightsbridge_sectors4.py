import sqlite3
from pathlib import Path

from src.sector_mappings.knightsbridge import KNIGHTSBRIDGE_SECTOR_MAP

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

def map_knightsbridge_sector(sector_raw: str):
    mapping = KNIGHTSBRIDGE_SECTOR_MAP.get(sector_raw)
    if not mapping:
        raise RuntimeError(f"UNMAPPED_KNIGHTSBRIDGE_SECTOR: {sector_raw}")

    return (
        mapping["industry"],
        mapping["sector"],
        mapping["confidence"],
        mapping["reason"],
    )


def backfill_knightsbridge_sectors(dry_run: bool = False):
    print(f"📀 SQLite DB path: {DB_PATH}")
    print(f"🧪 DRY_RUN: {dry_run}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT id, source_listing_id, sector_raw
        FROM deals
        WHERE source = 'Knightsbridge'
          AND (industry IS NULL OR sector IS NULL)
        ORDER BY source_listing_id
        """
    ).fetchall()

    if not rows:
        print("✅ Nothing to backfill")
        return

    updated = 0

    try:
        for r in rows:
            deal_id = r["id"]
            sector_raw = r["sector_raw"]

            industry, sector, confidence, reason = map_knightsbridge_sector(
                sector_raw
            )

            print(
                f"KB {r['source_listing_id']}: "
                f"{sector_raw} → {industry} / {sector}"
            )

            if not dry_run:
                conn.execute(
                    """
                    UPDATE deals
                    SET industry                    = ?,
                        sector                      = ?,
                        sector_source               = 'broker',
                        sector_inference_confidence = ?,
                        sector_inference_reason     = ?
                    WHERE id = ?
                    """,
                    (
                        industry,
                        sector,
                        confidence,
                        reason,
                        deal_id,
                    ),
                )
                updated += 1

        if not dry_run:
            conn.commit()

        print(f"✅ Backfill complete — updated={updated}")

    finally:
        conn.close()


if __name__ == "__main__":
    backfill_knightsbridge_sectors(dry_run=False)