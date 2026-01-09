import sqlite3
import time
import random
import hashlib
import json
from datetime import datetime
from pathlib import Path

from playwright._impl._errors import Error as PlaywrightError

from src.brokers.dealopportunities_client import DealOpportunitiesClient
from src.sector_mappings.dealopportunities import map_dealopportunities_sector
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

# =========================================================
# CONFIG
# =========================================================

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

MAX_RUNTIME = 5 * 60          # GitHub-safe
BROWSER_RESET_EVERY = 25
HUMAN_SLEEP_BASE = 4
HUMAN_SLEEP_JITTER = 4

DRY_RUN = False


# =========================================================
# HELPERS
# =========================================================

def human_sleep():
    time.sleep(HUMAN_SLEEP_BASE + random.random() * HUMAN_SLEEP_JITTER)


# =========================================================
# MAIN
# =========================================================

def enrich_dealopportunities():
    print("=" * 72)
    print("🧠 DealOpportunities Incremental Enrichment")
    print(f"📀 SQLite DB : {DB_PATH}")
    print(f"🧪 DRY_RUN   : {DRY_RUN}")
    print("=" * 72)

    start_time = time.time()
    processed = 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            source_url,
            title,
            sector_raw,
            detail_fetched_at
        FROM deals
        WHERE source = 'DealOpportunities'
          AND (
            detail_fetched_at IS NULL
            OR detail_fetched_at < DATE('now', '-7 days')
          )
        ORDER BY
            detail_fetched_at IS NOT NULL,
            detail_fetched_at ASC,
            last_seen DESC
        """
    ).fetchall()

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    client = DealOpportunitiesClient()
    client.start()

    try:
        for idx, r in enumerate(rows):
            if time.time() - start_time > MAX_RUNTIME:
                print("⏱️ Time limit reached — exiting cleanly")
                break

            if processed > 0 and processed % BROWSER_RESET_EVERY == 0:
                print("🔄 Browser recycle")
                client.stop()
                human_sleep()
                client.start()

            deal_id = r["id"]
            deal_key = r["source_listing_id"]
            url = r["source_url"]

            print(f"\n[{processed + 1}] ➜ {deal_key}")

            pdf_path = PDF_ROOT / f"{deal_key}.pdf"

            try:
                html = client.fetch_listing_detail_and_pdf(
                    url=url,
                    pdf_path=pdf_path,
                )
            except PlaywrightError as e:
                conn.execute(
                    """
                    UPDATE deals
                    SET pdf_error = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (str(e)[:500], deal_id),
                )
                conn.commit()
                print("❌ Fetch failed")
                continue

            content_hash = hashlib.sha256(html.encode()).hexdigest()

            # -------------------------------------------------
            # SECTOR / INDUSTRY (AUTHORITATIVE)
            # -------------------------------------------------
            mapping = map_dealopportunities_sector(
                raw_sector=r["sector_raw"]
            )

            industry = mapping["industry"]
            sector = mapping["sector"]

            # -------------------------------------------------
            # DRIVE
            # -------------------------------------------------
            parent_id = get_drive_parent_folder_id(
                industry=industry,
                broker="DealOpportunities",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_id,
                deal_id=deal_key,
                deal_title=r["title"],
            )

            drive_folder_url = (
                f"https://drive.google.com/drive/folders/{deal_folder_id}"
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{deal_key}.pdf",
                folder_id=deal_folder_id,
            )

            # -------------------------------------------------
            # DB UPDATE (ATOMIC)
            # -------------------------------------------------
            if not DRY_RUN:
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        industry = ?,
                        sector = ?,
                        content_hash = ?,
                        drive_folder_id = ?,
                        drive_folder_url = ?,
                        pdf_drive_url = ?,
                        pdf_generated_at = CURRENT_TIMESTAMP,
                        detail_fetched_at = CURRENT_TIMESTAMP,
                        last_updated = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                    """,
                    (
                        industry,
                        sector,
                        content_hash,
                        deal_folder_id,
                        drive_folder_url,
                        pdf_drive_url,
                        deal_id,
                    ),
                )
                conn.commit()

            pdf_path.unlink(missing_ok=True)
            processed += 1
            human_sleep()

            print("✅ Enriched")

    finally:
        client.stop()
        conn.close()

    print(f"\n🏁 Completed — deals processed: {processed}")


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    enrich_dealopportunities()