"""
Daltons importer

Contract:
- Capture raw data only (no inference)
- Idempotent
- DRY_RUN supported
- Daltons is an AGGREGATOR
"""

import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.brokers.daltons_client import DaltonsClient
from src.persistence.repository import SQLiteRepository

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

SOURCE = "Daltons"
BASE_URL = "https://www.daltonsbusiness.com"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
MAX_PAGES = int(os.getenv("DALTONS_MAX_PAGES", "50"))
SLEEP_SECS = float(os.getenv("DALTONS_SLEEP", "1.2"))

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def extract_listing_id(url: str) -> str:
    """
    Daltons URLs contain DB<digits>.
    Example:
    /listing/...-DB2480562/
    """
    m = re.search(r"DB(\d+)", url)
    if m:
        return m.group(1)

    # Fallback: stable slug
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


def extract_sector_raw(soup: BeautifulSoup) -> str | None:
    """
    Capture Daltons taxonomy verbatim.
    Breadcrumbs are richest and deterministic.
    """

    crumb = soup.select_one(".breadcrumbs, .breadcrumb")
    if crumb:
        txt = crumb.get_text(" ", strip=True)
        parts = [p.strip() for p in txt.split(">")]
        if parts:
            return " ".join(parts)

    cat = soup.select_one(".category, .listing-category")
    if cat:
        return cat.get_text(strip=True)

    return None


# -------------------------------------------------------------------
# Parsers
# -------------------------------------------------------------------

def parse_index(html: str) -> list[dict]:
    """
    Daltons index pages do not expose stable card classes.
    The only invariant is links containing `/listing/`.
    """

    soup = BeautifulSoup(html, "html.parser")

    seen = set()
    out = []

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/listing/" not in href:
            continue

        url = urljoin(BASE_URL, href)

        if url in seen:
            continue
        seen.add(url)

        out.append({
            "url": url,
            "listing_id": extract_listing_id(url),
        })

    return out


def parse_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.select_one("h1, h2.item-title")
    title = title_el.get_text(strip=True) if title_el else None

    loc_el = soup.select_one(".loc-urls-wrap")
    location_raw = loc_el.get_text(" ", strip=True) if loc_el else None

    sector_raw = extract_sector_raw(soup)

    return {
        "title": title,
        "sector_raw": sector_raw,
        "industry_raw": None,
        "location_raw": location_raw,
    }


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    repo = SQLiteRepository(DB_PATH)
    client = DaltonsClient()

    print(f"🏷️ Daltons import starting | DRY_RUN={DRY_RUN}")

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

            url = rec["url"]
            listing_id = rec["listing_id"]

            # Idempotency: Daltons-native identity
            if repo.deal_exists(SOURCE, listing_id):
                continue

            detail_html = client.detail_page(url)
            parsed = parse_detail(detail_html)

            deal = {
                # Identity
                "source": SOURCE,
                "source_listing_id": listing_id,
                "source_url": url,

                # Core descriptors
                "title": parsed["title"],

                # Raw taxonomy (verbatim, multi-level)
                "sector_raw": parsed["sector_raw"],
                "industry_raw": parsed["industry_raw"],

                # Canonical fields left NULL
                "sector": None,
                "industry": None,

                # Location
                "location_raw": parsed["location_raw"],
                "location": None,

                # Aggregator identity (CORRECT)
                "canonical_external_id": listing_id,
                "broker_name": None,
                "broker_listing_url": None,
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
        f"✅ Daltons import complete | "
        f"seen={total_seen} inserted={total_inserted}"
    )


if __name__ == "__main__":
    main()