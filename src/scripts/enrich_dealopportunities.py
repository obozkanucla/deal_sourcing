import sqlite3
import time
import random
import hashlib
import json
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright._impl._errors import Error as PlaywrightError

from src.brokers.dealopportunities_client import DealOpportunitiesClient
from src.sector_mappings.dealopportunities import map_dealopportunities_sector
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    get_drive_service,
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

# =========================================================
# CONFIG (unchanged behavior)
# =========================================================

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

MAX_RUNTIME = 2 * 60            # GitHub-safe
BROWSER_RESET_EVERY = 25

HUMAN_SLEEP_BASE = 4            # protection layer 3
HUMAN_SLEEP_JITTER = 4

DRY_RUN = False


# =========================================================
# HUMAN THROTTLING (layer 3)
# =========================================================

def human_sleep():
    time.sleep(HUMAN_SLEEP_BASE + random.random() * HUMAN_SLEEP_JITTER)


# =========================================================
# HTML PARSING (RESTORED)
# =========================================================

def parse_do_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # -------- description --------
    desc_el = soup.select_one(".opportunity-description, .content, article")
    description = desc_el.get_text("\n", strip=True) if desc_el else None

    # -------- facts --------
    facts = {}
    for dt in soup.select("dl dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        value = dd.get_text(strip=True) if dd else None
        if not value:
            continue

        if "region" in label:
            facts["location"] = value

    return {
        "description": description,
        "facts": facts,
    }


def extract_do_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    return h1.get_text(strip=True) if h1 else None


# =========================================================
# MAIN — FULL REPAIR RUN
# =========================================================

def enrich_dealopportunities():
    print("=" * 72)
    print("🧠 DealOpportunities FULL REPAIR ENRICHMENT")
    print(f"📀 SQLite DB : {DB_PATH}")
    print(f"🧪 DRY_RUN   : {DRY_RUN}")
    print("=" * 72)

    start_time = time.time()
    processed = 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 🔧 ONE-TIME REPAIR: ALL DO DEALS
    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            source_url,
            title,
            sector_raw
        FROM deals
        WHERE source = 'DealOpportunities'
        ORDER BY last_seen DESC
        """
    ).fetchall()

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    client = DealOpportunitiesClient()
    client.start()

    drive_service = get_drive_service()

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
            print(f"➡️ Fetching detail page:\n   {url}")

            pdf_path = PDF_ROOT / f"{deal_key}.pdf"

            # -------- SINGLE FETCH (NO DUPLICATION) --------
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

            parsed = parse_do_detail(html)
            title = extract_do_title(html) or r["title"]
            description = parsed["description"]
            location_raw = parsed["facts"].get("location")

            content_hash = hashlib.sha256(html.encode()).hexdigest()

            # -------- INDUSTRY / SECTOR (AUTHORITATIVE, RESTORED) --------
            mapping = map_dealopportunities_sector(
                raw_sector=r["sector_raw"]
            )

            industry = mapping["industry"]
            sector = mapping["sector"]

            # -------- DRIVE (IDEMPOTENT) --------
            parent_id = get_drive_parent_folder_id(
                industry=industry,
                broker="DealOpportunities",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_id,
                deal_id=deal_key,
                deal_title=title,
            )

            drive_folder_url = (
                f"https://drive.google.com/drive/folders/{deal_folder_id}"
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{deal_key}.pdf",
                folder_id=deal_folder_id,
            )

            # -------- DB UPDATE (ATOMIC, FULL REPAIR) --------
            if not DRY_RUN:
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        title = ?,
                        description = ?,
                        location_raw = ?,
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
                        title,
                        description,
                        location_raw,
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