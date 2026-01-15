import sqlite3
from pathlib import Path

from src.sector_mappings.knightsbridge import (
    resolve_knightsbridge_sector,
)

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"


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
          AND (
                industry IS NULL
             OR sector_source IS NULL
          )
        ORDER BY source_listing_id
        """
    ).fetchall()

    if not rows:
        print("✅ Nothing to backfill")
        conn.close()
        return

    updated = 0

    try:
        for r in rows:
            (
                industry,
                sector,
                sector_source,
                confidence,
                reason,
            ) = resolve_knightsbridge_sector(r["sector_raw"])

            print(
                f"KB {r['source_listing_id']}: "
                f"{r['sector_raw']} → {industry} / {sector} [{sector_source}]"
            )

            if not dry_run:
                conn.execute(
                    """
                    UPDATE deals
                    SET industry                    = ?,
                        sector                      = ?,
                        sector_source               = ?,
                        sector_inference_confidence = ?,
                        sector_inference_reason     = ?
                    WHERE id = ?
                    """,
                    (
                        industry,
                        sector,
                        sector_source,
                        confidence,
                        reason,
                        r["id"],
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