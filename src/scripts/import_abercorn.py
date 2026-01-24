# src/scripts/import_abercorn.py

import sqlite3
from pathlib import Path
from datetime import datetime

from src.brokers.abercorn_client import AbercornClient
from src.domain.industries import CANONICAL_INDUSTRIES
from src.sector_mappings.abercorn import resolve_abercorn_sector

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
SOURCE = "Abercorn"

DRY_RUN = False  # flip when happy


def import_abercorn():
    print(f"📀 SQLite DB path: {DB_PATH}")

    client = AbercornClient(headless=True)
    client.start()

    try:
        rows = client.fetch_index()
    finally:
        client.stop()

    if not rows:
        print("✅ Nothing to import")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    inserted = 0
    skipped = 0

    try:
        for r in rows:
            listing_id = r["source_listing_id"]

            # ----------------------------------------------------------
            # Sector resolution via REF PREFIX (AUTHORITATIVE)
            # ----------------------------------------------------------
            import re

            ref_prefix = re.match(r"[A-Z]+", listing_id).group()

            (
                industry,
                sector,
                sector_source,
                confidence,
                reason,
            ) = resolve_abercorn_sector(ref_prefix)

            if industry not in CANONICAL_INDUSTRIES:
                raise RuntimeError(f"ILLEGAL_INDUSTRY_STATE: {industry}")

            exists = conn.execute(
                """
                SELECT 1 FROM deals
                WHERE source = ?
                  AND source_listing_id = ?
                """,
                (SOURCE, listing_id),
            ).fetchone()

            if exists:
                skipped += 1
                continue

            if DRY_RUN:
                print(
                    "DRY_RUN → INSERT",
                    listing_id,
                    "|",
                    industry,
                    "|",
                    sector,
                )
                inserted += 1
                continue

            conn.execute(
                """
                INSERT INTO deals (
                    source,
                    source_listing_id,
                    source_url,
                    title,
                    industry,
                    sector,
                    sector_source,
                    sector_inference_confidence,
                    sector_inference_reason,
                    needs_detail_refresh,
                    last_updated,
                    last_updated_source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'AUTO')
                """,
                (
                    SOURCE,
                    listing_id,
                    r["source_url"],
                    r["title"],
                    industry,
                    sector,
                    sector_source,
                    confidence,
                    reason,
                    datetime.today().isoformat(),
                ),
            )
            inserted += 1

        if not DRY_RUN:
            conn.commit()

    finally:
        conn.close()

    print(
        f"\n🏁 Abercorn import complete — "
        f"{inserted} inserted, {skipped} skipped"
    )


if __name__ == "__main__":
    import_abercorn()