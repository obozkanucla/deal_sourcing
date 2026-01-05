# src/scripts/enrich_businesses4sale.py

from pathlib import Path
import time
import random
import hashlib

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

from src.persistence.repository import SQLiteRepository
from src.enrichment.financial_extractor import extract_financial_metrics

# -----------------------------------
# CONFIG
# -----------------------------------

DRY_RUN = True          # 🔒 flip to False when confident
MAX_DEALS = 20
SLEEP_BETWEEN = (3, 6)

DETAIL_WAIT_SELECTOR = "#hero, div.teaser-content"

# -----------------------------------
# HELPERS
# -----------------------------------

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None


def extract_teaser_field(soup, heading: str) -> str | None:
    """
    Extracts text from a <div class="teaser-field"> by <h3> title
    """
    for field in soup.select("div.teaser-field"):
        h3 = field.select_one("h3")
        if h3 and h3.get_text(strip=True).lower() == heading.lower():
            return text_or_none(field)
    return None

def extract_mv_id(soup) -> str | None:
    p = soup.select_one("div.teaser-ref p")
    if not p:
        return None

    txt = p.get_text(strip=True)
    if txt.startswith("ID: MV"):
        return txt.replace("ID:", "").strip()

    return None

def compute_content_hash(*parts: str | None) -> str:
    h = hashlib.sha256()
    for p in parts:
        if p:
            h.update(p.encode("utf-8"))
    return h.hexdigest()


# -----------------------------------
# MAIN
# -----------------------------------

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_deals_needing_details_for_source(
        source="BusinessesForSale",
        limit=MAX_DEALS,
    )

    print(f"🔍 Deals to enrich: {len(deals)}")

    enriched = 0

    for i, deal in enumerate(deals, start=1):
        url = deal["source_url"]
        slug = deal["source_listing_id"]

        print(f"\n➡️ [{i}/{len(deals)}] {slug}")
        print(f"🌐 {url}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            try:
                page.goto(url, timeout=60_000)
                page.wait_for_selector(DETAIL_WAIT_SELECTOR, timeout=20_000)
            except TimeoutError:
                print("⚠️ Detail selector not found — skipping")
                browser.close()
                continue

            soup = BeautifulSoup(page.content(), "html.parser")

            mv_id = extract_mv_id(soup)

            # -----------------------------------
            # HERO
            # -----------------------------------

            title = text_or_none(soup.select_one("#hero h1"))
            location = text_or_none(soup.select_one("#hero p.location"))
            hero_desc = text_or_none(soup.select_one("#hero p"))

            # -----------------------------------
            # DESCRIPTION
            # -----------------------------------

            business_desc = extract_teaser_field(soup, "Business Description")
            description = business_desc or hero_desc

            # -----------------------------------
            # FINANCIALS (UK + INTL)
            # -----------------------------------

            financial_chunks: list[str] = []

            # (1) UK inline financials
            for dl in soup.select("div.financials dl"):
                dt = dl.select_one("dt")
                dd = dl.select_one("dd")
                if dt and dd:
                    financial_chunks.append(
                        f"{dt.get_text(strip=True)} {dd.get_text(strip=True)}"
                    )

            # (2) Narrative financial summary
            financial_summary = extract_teaser_field(soup, "Financial Summary")
            if financial_summary:
                financial_chunks.append(financial_summary)

            financial_text = " ".join(financial_chunks).strip() or None

            extracted = extract_financial_metrics(financial_text or "")

            # -----------------------------------
            # CONTENT HASH
            # -----------------------------------

            content_hash = compute_content_hash(
                title,
                location,
                description,
                financial_text,
            )

            # -----------------------------------
            # UPDATES
            # -----------------------------------

            updates = {}
            lookup_listing_id = deal["source_listing_id"]

            if mv_id and deal["source_listing_id"] != mv_id:
                updates["source_listing_id"] = mv_id
                print(f"🔁 Canonicalizing ID {deal['source_listing_id']} → {mv_id}")

            if title:
                updates["title"] = title

            if location:
                updates["location"] = location

            if description:
                updates["description"] = description

            updates["content_hash"] = content_hash
            updates["needs_detail_refresh"] = 0

            for field in ("revenue_k", "ebitda_k", "asking_price_k"):
                val = extracted.get(field)
                if val and deal.get(field) is None:
                    updates[field] = val["value"]

            if updates:
                print("✍️ UPDATES:", updates)

                if not DRY_RUN:
                    repo.update_detail_fields_by_source(
                        source=deal["source"],
                        source_listing_id=lookup_listing_id,  # always the ORIGINAL key
                        fields=updates,
                    )
                    enriched += 1

            browser.close()

            time.sleep(random.uniform(*SLEEP_BETWEEN))

    print(f"\n✅ BusinessesForSale enrichment complete — updated {enriched} deals")

if __name__ == "__main__":
    main()