"""
Business Sale Report (BSR) importer

Contract:
- Capture raw data only (no inference)
- Index pages only (detail pages may be gated)
- Idempotent
- DRY_RUN supported
"""

import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.brokers.bsr_client import BusinessSaleReportClient
from src.persistence.repository import SQLiteRepository


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

SOURCE = "BusinessSaleReport"
BASE_URL = "https://www.business-sale.com"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = os.getenv("DRY_RUN", "1") == "0"
MAX_PAGES = int(os.getenv("BSR_MAX_PAGES", "500"))
SLEEP_SECS = float(os.getenv("BSR_SLEEP", "1.0"))


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def extract_listing_id(url: str) -> str:
    """
    Extract a stable BSR listing identifier.
    Example:
    /companies-for-sale/dual-venue-bar-and-restaurant-678890
    """
    m = re.search(r"-(\d{4,})/?$", url)
    if not m:
        raise ValueError(f"Cannot extract listing id from {url}")
    return m.group(1)


# -------------------------------------------------------------------
# Parsers
# -------------------------------------------------------------------

def parse_index(html: str) -> list[dict]:
    """
    Parse BSR index page.
    Sector is NOT available here.
    """
    soup = BeautifulSoup(html, "html.parser")
    out = []

    for card in soup.select("div.card-body"):
        a = card.select_one("h5.card-title a[href]")
        if not a:
            continue

        url = urljoin(BASE_URL, a["href"])
        listing_id = extract_listing_id(url)

        title = a.get_text(strip=True)

        location = None
        loc_el = card.select_one("h6.card-subtitle")
        if loc_el:
            location = loc_el.get_text(strip=True)

        out.append({
            "listing_id": listing_id,
            "url": url,
            "title": title,
            "location_raw": location,
        })

    return out


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    repo = SQLiteRepository(DB_PATH)
    client = BusinessSaleReportClient()

    print(f"🏷️ BSR import starting | DRY_RUN={DRY_RUN}")

    total_seen = 0
    total_inserted = 0

    for page in range(1, MAX_PAGES + 1):
        print(f"📄 Index page {page}")

        html = client.list_page(page)
        listings = parse_index(html)

        if not listings:
            print("🛑 No listings found, stopping")
            break

        for rec in listings:
            total_seen += 1

            listing_id = rec["listing_id"]
            url = rec["url"]

            # Idempotency guard
            if repo.deal_exists(SOURCE, listing_id):
                continue

            deal = {
                # Identity
                "source": SOURCE,
                "source_listing_id": listing_id,
                "source_url": url,

                # Core descriptors
                "title": rec["title"],

                # Taxonomy deferred to enrichment
                "sector_raw": None,
                "industry": None,
                "sector": None,

                # Location
                "location_raw": rec["location_raw"],
                "location": None,

                # Aggregator metadata
                "canonical_external_id": listing_id,   # stable BSR ref
                "broker_name": "Business Sale",
                "broker_listing_url": url,
                "source_role": "AGGREGATOR",
            }

            if DRY_RUN:
                print("🧪 DRY_RUN deal:", deal)
            else:
                repo.upsert_deal_v2(deal)
                total_inserted += 1

            time.sleep(SLEEP_SECS)

        time.sleep(SLEEP_SECS * 2)

    print(
        f"✅ BSR import complete | "
        f"seen={total_seen} inserted={total_inserted}"
    )


if __name__ == "__main__":
    main()