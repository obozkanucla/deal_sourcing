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
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

# =========================================================
# CONFIG
# =========================================================

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

MAX_RUNTIME = 2 * 60          # GitHub-safe
BROWSER_RESET_EVERY = 25

HUMAN_SLEEP_BASE = 4
HUMAN_SLEEP_JITTER = 4

DRY_RUN = False

# =========================================================
# HUMAN BEHAVIOUR
# =========================================================

def human_sleep():
    time.sleep(HUMAN_SLEEP_BASE + random.random() * HUMAN_SLEEP_JITTER)

# =========================================================
# HTML PARSING (RESTORED)
# =========================================================

def extract_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1 > a[href*='/opportunity/']")
    return h1.get_text(strip=True) if h1 else None


def extract_description(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    for selector in [
        ".opportunity-description",
        ".content",
        "article",
    ]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text("\n", strip=True)
            if text:
                return text

    legacy = soup.select_one("table td[valign='top']")
    if legacy:
        return legacy.get_text("\n", strip=True)

    return None


def extract_facts(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    facts = {}

    for dt in soup.select("dl dt"):
        key = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue

        value = dd.get_text(strip=True)
        if not value:
            continue

        if "sector" in key:
            facts["sector_raw"] = value
        elif "region" in key or "location" in key or "area" in key:
            facts["location_raw"] = value

    return facts

# =========================================================
# MAIN
# =========================================================

def enrich_dealopportunities():
    print("=" * 72)
    print("🧠 DealOpportunities Incremental Enrichment (FULL)")
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
            location_raw,
            description,
            detail_fetched_at
        FROM deals
        WHERE source = 'DealOpportunities'
          AND (
                detail_fetched_at IS NULL
             OR title IS NULL
             OR description IS NULL
             OR location_raw IS NULL
             OR industry IS NULL
             OR sector IS NULL
             OR pdf_drive_url IS NULL
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
        for r in rows:
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

            title = extract_title(html)
            description = extract_description(html)
            facts = extract_facts(html)

            raw_sector = facts.get("sector_raw") or r["sector_raw"]
            location_raw = facts.get("location_raw") or r["location_raw"]

            mapping = map_dealopportunities_sector(raw_sector=raw_sector)

            industry = mapping["industry"]
            sector = mapping["sector"]

            parent_id = get_drive_parent_folder_id(
                industry=industry,
                broker="DealOpportunities",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_id,
                deal_id=deal_key,
                deal_title=title or deal_key,
            )

            drive_folder_url = (
                f"https://drive.google.com/drive/folders/{deal_folder_id}"
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{deal_key}.pdf",
                folder_id=deal_folder_id,
            )

            if not DRY_RUN:
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        title = ?,
                        description = ?,
                        sector_raw = ?,
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
                        raw_sector,
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