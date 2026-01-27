"""
BusinessesForSaleFree (B4SFree) importer

Contract:
- Capture raw data only (no inference)
- Index pages only
- Idempotent
- DRY_RUN supported
"""

import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src._to_delete.b4sfree_client import B4SFreeClient
from src.persistence.repository import SQLiteRepository


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

SOURCE = "B4SFree"
BASE_URL = "https://businessesforsalefree.com"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
MAX_PAGES = int(os.getenv("B4SFREE_MAX_PAGES", "50"))
SLEEP_SECS = float(os.getenv("B4SFREE_SLEEP", "1.0"))


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def extract_listing_id(url: str) -> str:
    """
    firm-for-sale-full-details-35224.htm → 35224
    """
    m = re.search(r"(\d+)\.htm$", url)
    if m:
        return m.group(1)

    path = urlparse(url).path.rstrip("/")
    return path.split("-")[-1]


def extract_sector_raw(card: BeautifulSoup) -> str | None:
    """
    Builds hierarchical sector string from Primary sector links.
    Example:
    Construction & Maintenance > Building Services
    """

    links = card.select("div.listing-details a")
    sectors = []

    for a in links:
        href = a.get("href", "")
        if "sector=" in href or "subsec=" in href:
            txt = a.get_text(strip=True)
            if txt:
                sectors.append(txt)

    if sectors:
        return " > ".join(sectors)

    return None


def extract_location_raw(card: BeautifulSoup) -> str | None:
    el = card.select_one("div.listing-details")
    if not el:
        return None

    txt = el.get_text(" ", strip=True)
    if txt.lower().startswith("location:"):
        return txt.replace("Location:", "").strip()

    return None


# -------------------------------------------------------------------
# Parsers
# -------------------------------------------------------------------

def parse_index(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out = []

    # IMPORTANT: handle BOTH card variants
    cards = soup.select("div.listing-item-first, div.listing-item")

    for card in cards:
        a = card.select_one("a.listing-title-link[href]")
        if not a:
            continue

        url = urljoin(BASE_URL, a["href"])
        listing_id = extract_listing_id(url)
        title = a.get_text(strip=True)

        out.append({
            "listing_id": listing_id,
            "url": url,
            "title": title,
            "sector_raw": extract_sector_raw(card),
            "location_raw": extract_location_raw(card),
        })

    return out


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    repo = SQLiteRepository(DB_PATH)
    client = B4SFreeClient()

    print(f"🏷️ B4SFree import starting | DRY_RUN={DRY_RUN}")

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

            if repo.deal_exists(SOURCE, listing_id):
                continue

            deal = {
                # Identity
                "source": SOURCE,
                "source_listing_id": listing_id,
                "source_url": url,

                # Core descriptors
                "title": rec["title"],

                # Raw taxonomy
                "sector_raw": rec["sector_raw"],
                "industry_raw": None,

                # Canonical fields
                "sector": None,
                "industry": None,

                # Location
                "location_raw": rec["location_raw"],
                "location": None,

                # Aggregator metadata
                "canonical_external_id": listing_id,
                "broker_name": "BusinessesForSaleFree",
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
        f"✅ B4SFree import complete | "
        f"seen={total_seen} inserted={total_inserted}"
    )


if __name__ == "__main__":
    main()