"""
Daltons importer

Contract:
- Capture raw data only
- Prevent broker duplicates
- Idempotent
- DRY_RUN supported
"""

import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.brokers.daltons_client import DaltonsClient
from src.persistence.repository import SQLiteRepository

SOURCE = "Daltons"
BASE_URL = "https://www.daltonsbusiness.com"
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
MAX_PAGES = int(os.getenv("DALTONS_MAX_PAGES", "50"))
SLEEP_SECS = float(os.getenv("DALTONS_SLEEP", "1.2"))


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def extract_listing_id(url: str) -> str:
    m = re.search(r"DB(\d+)", url)
    return m.group(1) if m else urlparse(url).path.rstrip("/").split("/")[-1]


def extract_sector_raw(soup: BeautifulSoup) -> str | None:
    crumb = soup.select_one(".breadcrumbs, .breadcrumb")
    if crumb:
        return crumb.get_text(" ", strip=True)

    return None


def extract_broker_info(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    """
    Daltons sometimes exposes broker attribution.
    Keep raw, do not infer.
    """
    broker = soup.find(string=re.compile("Broker", re.I))
    if not broker:
        return None, None

    link = broker.find_parent("a")
    return (
        broker.strip() if broker else None,
        link["href"] if link and link.has_attr("href") else None,
    )


# --------------------------------------------------
# Parsers
# --------------------------------------------------

def parse_index(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    urls = []

    for a in soup.find_all("a", href=True):
        if "/listing/" not in a["href"]:
            continue
        url = urljoin(BASE_URL, a["href"])
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def parse_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.select_one("h1, h2.item-title")
    location_el = soup.select_one(".loc-urls-wrap")

    broker_name, broker_url = extract_broker_info(soup)

    return {
        "title": title_el.get_text(strip=True) if title_el else None,
        "sector_raw": extract_sector_raw(soup),
        "industry_raw": None,
        "location_raw": location_el.get_text(" ", strip=True) if location_el else None,
        "broker_name": broker_name,
        "broker_listing_url": broker_url,
    }


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    repo = SQLiteRepository(DB_PATH)
    client = DaltonsClient()

    print(f"🏷️ Daltons import starting | DRY_RUN={DRY_RUN}")

    seen = inserted = skipped = 0

    for page in range(1, MAX_PAGES + 1):
        print(f"📄 Index page {page}")
        urls = parse_index(client.list_page(page))

        if not urls:
            break

        for url in urls:
            seen += 1
            listing_id = extract_listing_id(url)

            # Idempotency (Daltons-level)
            if repo.deal_exists(SOURCE, listing_id):
                skipped += 1
                continue

            detail = parse_detail(client.detail_page(url))

            # Tier 1 / 2 resolution
            if detail["broker_name"] and detail["broker_listing_url"]:
                existing = repo.find_primary_by_url(detail["broker_listing_url"])
                if existing:
                    print("🔗 Resolved to existing PRIMARY, skipping insert")
                    skipped += 1
                    continue

            deal = {
                "source": SOURCE,
                "source_listing_id": listing_id,
                "source_url": url,
                "source_role": "AGGREGATOR",

                "canonical_external_id": listing_id,

                "broker_name": detail["broker_name"],
                "broker_listing_url": detail["broker_listing_url"],

                "title": detail["title"],
                "sector_raw": detail["sector_raw"],
                "industry_raw": detail["industry_raw"],

                "sector": None,
                "industry": None,

                "location_raw": detail["location_raw"],
                "location": None,
            }

            if DRY_RUN:
                print("🧪 DRY_RUN deal:", deal)
            else:
                repo.upsert_deal_v2(deal)
                inserted += 1

            time.sleep(SLEEP_SECS)

    print(f"✅ Daltons import complete | seen={seen} inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()