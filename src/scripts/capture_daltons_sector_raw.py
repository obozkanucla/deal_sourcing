"""
Daltons sector_raw capture (DRY, no DB writes)

Purpose:
- Crawl Daltons detail pages
- Extract sector_raw breadcrumb
- Emit CSV for mapping construction
"""

import time
import csv
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

from src.persistence.repository import SQLiteRepository

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "Daltons"
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

OUT_CSV = Path("/tmp/daltons_sector_raw_capture.csv")

DETAIL_WAIT_SELECTOR = "body"
HEADLESS = True
SLEEP_SECS = 0.5

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def extract_daltons_sector_raw(soup: BeautifulSoup) -> Optional[str]:
    crumbs = soup.select("ol.breadcrumb li a span, ol.breadcrumb li span")
    if not crumbs:
        return None

    parts = [c.get_text(strip=True) for c in crumbs]

    if parts and parts[0].lower() == "home":
        parts = parts[1:]

    if len(parts) >= 2:
        return " > ".join(parts)

    return None

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def capture():
    print(f"📀 SQLite DB path: {DB_PATH}")
    print("🏷️ Daltons sector_raw capture starting (DRY)")

    repo = SQLiteRepository(DB_PATH)
    deals = repo.fetch_deals_for_enrichment(source=SOURCE)

    print(f"🔍 {len(deals)} Daltons deals to scan")

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source_listing_id", "sector_raw"],
        )
        writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)

            try:
                for i, deal in enumerate(deals, start=1):
                    listing_id = deal["source_listing_id"]
                    url = deal["source_url"]

                    print(f"[{i}/{len(deals)}] {listing_id}")

                    context = browser.new_context()
                    page = context.new_page()

                    try:
                        page.goto(url, timeout=60_000)
                        page.wait_for_selector(DETAIL_WAIT_SELECTOR, timeout=20_000)
                    except TimeoutError:
                        context.close()
                        continue

                    soup = BeautifulSoup(page.content(), "html.parser")
                    sector_raw = extract_daltons_sector_raw(soup)

                    writer.writerow(
                        {
                            "source_listing_id": listing_id,
                            "sector_raw": sector_raw,
                        }
                    )

                    context.close()
                    time.sleep(SLEEP_SECS)

            finally:
                browser.close()

    print(f"✅ CSV written: {OUT_CSV}")

if __name__ == "__main__":
    capture()