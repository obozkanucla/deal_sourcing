# src/brokers/transworld_uk/import_transworld.py

from pathlib import Path
from bs4 import BeautifulSoup
from hashlib import sha1
import re
from datetime import datetime
from src.persistence.repository import SQLiteRepository
from src.brokers.transworld_client import TransworldUKClient

DRY_RUN = False

def is_valid_transworld_listing(slug: str | None, url: str) -> bool:
    if not slug:
        return False

    slug = slug.strip().lower()

    if slug == "listings":
        return False

    if url.rstrip("/").endswith("/listings"):
        return False

    return True

def parse_asking_price_k(text: str | None):
    if not text:
        return None
    t = text.lower().replace(",", "")
    m = re.search(r"([\d.]+)\s*(m|k)?", t)
    if not m:
        return None
    value = float(m.group(1))
    if m.group(2) == "m":
        return value * 1000
    return value


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    client = TransworldUKClient()

    print("🚀 import_transworld started")

    next_page_url = client.SEARCH_URL
    visited = set()
    records = []

    while next_page_url and next_page_url not in visited:
        visited.add(next_page_url)
        html = client.fetch_index_page(next_page_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.result-item.paginateresults")
        if not items:
            break

        for item in items:
            a_tag = item.find("a")
            if not a_tag:
                continue

            source_url = client.resolve_url(a_tag["href"])
            source_listing_id = a_tag["href"].rstrip("/").split("/")[-1]

            # --------------------------------------------
            # HARD GUARD — INVALID TRANSWORLD LISTINGS
            # --------------------------------------------
            if not is_valid_transworld_listing(source_listing_id, source_url):
                if DRY_RUN:
                    print("⛔ skipped invalid listing:", source_url)
                continue

            def text(sel):
                el = item.select_one(sel)
                return el.get_text(strip=True) if el else None

            title = text("h3.locname")

            location = price = sector_raw = None
            for li in item.select("ul li"):
                t = li.get_text(strip=True)
                if t.startswith("Location:"):
                    location = t.replace("Location:", "").strip()
                elif t.startswith("Asking Price:"):
                    price = t.replace("Asking Price:", "").strip()
                elif t.startswith("Category:"):
                    sector_raw = t.replace("Category:", "").strip()

            asking_price_k = parse_asking_price_k(price)

            blob = "|".join(str(x) for x in [title, location, price, sector_raw])
            content_hash = sha1(blob.encode("utf-8")).hexdigest()

            records.append({
                "source": "transworld_uk",
                "source_listing_id": source_listing_id,
                "source_url": source_url,
                "title": title,
                "sector_raw": sector_raw,
                "location_raw": location,
                "asking_price_k": asking_price_k,
                "status": "active",
                "content_hash": content_hash,
            })

        next_link = soup.find("a", string=lambda s: s and "Next" in s)
        next_page_url = client.resolve_url(next_link["href"]) if next_link else None

    print(f"📦 Records scraped: {len(records)}")
    for r in records[:5]:
        print("🧪", r)

    if DRY_RUN:
        print("\n🧪 DRY RUN — no database writes performed")
        return

    for r in records:
        now = datetime.utcnow().isoformat(timespec="seconds")
        existing = repo.get_conn().execute(
            """
            SELECT 1
            FROM deals
            WHERE source = ?
              AND source_url = ? LIMIT 1
            """,
            ("transworld_uk", r["source_url"]),
        ).fetchone()

        if existing:
            continue

        repo.upsert_index_only(
            source=r["source"],
            source_listing_id=r["source_listing_id"],
            source_url=r["source_url"],
            title=r["title"],
            sector_raw=r.get("sector_raw"),
            location_raw=r.get("location_raw"),
            turnover_range_raw=None,
            first_seen=now,
            last_seen=now,
            last_updated=now,
            last_updated_source="IMPORT",
        )
    print(f"✅ Transworld index import complete: {len(records)}")


if __name__ == "__main__":
    main()