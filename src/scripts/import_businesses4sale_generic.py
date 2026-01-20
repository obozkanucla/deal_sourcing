# src/scripts/import_businesses4sale_generic.py
#
# DRY RUN – BusinessesForSale (Generic)
# Category-based crawl, Cloudflare-safe

import os
import time
import random
import sqlite3
from pathlib import Path
from typing import Dict, Set

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError
from src.sector_mappings.b4s import B4S_SECTOR_MAP
# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "BusinessesForSale_Generic"
DRY_RUN = False

B4S_CATEGORIES = {
    "agriculture": "https://uk.businessesforsale.com/uk/search/agriculture-businesses-for-sale",
    "energy": "https://uk.businessesforsale.com/uk/search/energy-businesses-for-sale",
    "engineering": "https://uk.businessesforsale.com/uk/search/engineering-businesses-for-sale",
    "commercial_property": "https://uk.businessesforsale.com/uk/search/commercial-property-for-sale",
    "food": "https://uk.businessesforsale.com/uk/search/food-businesses-for-sale",
    "manufacturing": "https://uk.businessesforsale.com/uk/search/manufacturing-businesses-for-sale",
    "services": "https://uk.businessesforsale.com/uk/search/services-businesses-for-sale",
    "wholesale_distribution": "https://uk.businessesforsale.com/uk/search/wholesale-and-distribution-businesses-for-sale",
}

MAX_PAGES = 50
SLEEP_BETWEEN_PAGES = 2

HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"

STORAGE_STATE = Path(".playwright/businesses4sale_search_state.json")
STORAGE_STATE.parent.mkdir(parents=True, exist_ok=True)

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = REPO_ROOT / "db" / "deals.sqlite"

# -------------------------------------------------
# IMPORT
# -------------------------------------------------

def import_businesses4sale_search() -> None:
    seen_urls: Set[str] = set()
    listings: Dict[str, dict] = {}

    # ---------------------------------------------
    # Preload existing B4S URLs for dedupe
    # ---------------------------------------------
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            """
            SELECT source_url
            FROM deals
            WHERE source LIKE 'BusinessesForSale%'
              AND source_url IS NOT NULL
            """
        ).fetchall()
        conn.close()

        seen_urls = {url for (url,) in rows}
        print(f"🧠 Loaded {len(seen_urls)} existing B4S URLs for dedupe")
    else:
        print("⚠️ DB not found — skipping dedupe preload")
    conn = sqlite3.connect(DB_PATH)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = _create_context(browser)
        page = context.new_page()

        # -----------------------------------------
        # SESSION WARM-UP (homepage only)
        # -----------------------------------------
        print("🔥 Warming Cloudflare session")
        page.goto("https://uk.businessesforsale.com", timeout=60_000)
        time.sleep(5)

        for category, base_url in B4S_CATEGORIES.items():
            print(f"\n📂 CATEGORY: {category}")
            mapping = B4S_SECTOR_MAP[category]  # ← THIS LINE
            page_num = 1

            while True:
                url = base_url if page_num == 1 else f"{base_url}-{page_num}"
                print(f"  🔍 PAGE {page_num}: {url}")

                page.goto(url, timeout=60_000, wait_until="domcontentloaded")

                try:
                    page.wait_for_selector("div.result, div.search-result", timeout=20_000)
                except TimeoutError:
                    print("  🛑 No results — stopping category")
                    break

                soup = BeautifulSoup(page.content(), "html.parser")
                blocks = soup.select("div.result, div.search-result")
                print(f"  🔎 Results found: {len(blocks)}")

                added = 0
                skipped = 0

                for block in blocks:
                    rec = _parse_search_block(block)
                    if not rec:
                        continue

                    url = rec["source_url"]

                    if url in seen_urls:
                        skipped += 1
                        print("duplicate found")
                        continue

                    seen_urls.add(url)
                    listings[url] = rec
                    added += 1
                    mapping = B4S_SECTOR_MAP[category]

                    if DRY_RUN:
                        print(
                            "DRY_RUN →",
                            rec["source_listing_id"],
                            category,
                            mapping["industry"],
                            mapping["sector"],
                            rec["source_url"],
                        )
                    else:
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
                                               needs_detail_refresh,
                                               first_seen,
                                               last_seen)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1,
                                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT(source, source_listing_id)
                            DO
                            UPDATE SET last_seen = CURRENT_TIMESTAMP
                            """,
                            (
                                SOURCE,
                                rec["source_listing_id"],
                                rec["source_url"],
                                rec["title"],
                                category,  # sector_raw
                                mapping["industry"],
                                mapping["sector"],
                                "broker",
                                mapping["confidence"],
                                mapping["reason"],
                            ),
                        )
                    print(f"    ➕ {rec['title'][:70]}")

                print(f"  ➕ New: {added} | 🔁 Skipped: {skipped}")

                if added == 0 or page_num >= MAX_PAGES:
                    break

                page_num += 1
                time.sleep(SLEEP_BETWEEN_PAGES + random.random() * 2)

        print(f"\n✅ DRY RUN COMPLETE — {len(listings)} net new listings")
        if not DRY_RUN:
            conn.commit()
        conn.close()
        context.storage_state(path=STORAGE_STATE)
        browser.close()

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def _create_context(browser):
    if STORAGE_STATE.exists():
        print("🔐 Reusing Cloudflare session")
        return browser.new_context(storage_state=STORAGE_STATE)

    print("🧠 New browser context")
    return browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )


def _parse_search_block(block) -> dict | None:
    a = block.select_one("h2 a")
    if not a:
        return None

    url = a.get("href")
    if not url:
        return None

    slug = url.rstrip("/").split("/")[-1]

    return {
        "source_listing_id": slug,
        "source_url": url,
        "title": a.get_text(strip=True),
    }

# -------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------

if __name__ == "__main__":
    import_businesses4sale_search()