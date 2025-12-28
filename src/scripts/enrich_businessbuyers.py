from pathlib import Path
import json
import hashlib
from bs4 import BeautifulSoup
import os
import re

from src.persistence.repository import SQLiteRepository
from src.brokers.businessbuyers_client import BusinessBuyersClient
from src.config import BB_USERNAME, BB_PASSWORD


# =========================
# CONFIG
# =========================

PDF_TMP_DIR = Path("/tmp/bb_pdfs")
PDF_TMP_DIR.mkdir(parents=True, exist_ok=True)

MAX_DEALS_PER_RUN = 2


# =========================
# PARSER
# =========================

def parse_bb_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # ---------- Asking price ----------
    asking_price = None
    asking_price_k = None

    # usually: <div class="price-ref"><p>£550,000</p><p>REF: ...</p></div>
    price_el = soup.select_one(".price-ref p")

    if price_el:
        raw = price_el.get_text(strip=True)

        # accept £, GBP, or numeric
        m = re.search(r"£?\s*([\d,]+)", raw)
        if m:
            asking_price = f"£{m.group(1)}"
            asking_price_k = float(m.group(1).replace(",", "")) / 1_000

    # ---------- Description ----------
    points = []
    for div in soup.select("#overview .selling-point"):
        text = div.get_text(" ", strip=True)
        if text:
            points.append(text)

    # de-duplicate while preserving order
    seen = set()
    unique_points = []
    for p in points:
        if p not in seen:
            unique_points.append(p)
            seen.add(p)

    description = "\n".join(unique_points) if unique_points else None

    return {
        "description": description,
        "asking_price": asking_price,
        "asking_price_k": asking_price_k,
        "facts": {},  # keep empty; BB facts are weak/unreliable
    }

# =========================
# MAIN
# =========================

def enrich_businessbuyers(limit: int = MAX_DEALS_PER_RUN, force=False):
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    if force:
        deals = repo.fetch_all(
            """
            SELECT *
            FROM deals
            WHERE source = 'BusinessBuyers'
            ORDER BY last_seen DESC
            """
        )
    else:
        deals = repo.fetch_deals_needing_details_for_source(
            "BusinessBuyers",
            limit if limit is not None else 10_000,
        )

    print(f"🔍 Found {len(deals)} BusinessBuyers deals needing details")

    if not deals:
        return

    bb = BusinessBuyersClient(
        username=BB_USERNAME,
        password=BB_PASSWORD,
        click_budget=None,
    )

    bb.login()

    for deal in deals:
        deal_id = deal["deal_id"]
        deal_key = deal["source_listing_id"]

        print(f"\n➡️ Enriching {deal_key}")
        print(deal["source_url"])

        # ensure cookie banner is gone before parsing
        bb.ensure_cookies_cleared()

        pdf_path = PDF_TMP_DIR / f"{deal_key}.pdf"

        # 1️⃣ TRY ANON FIRST (cheap, unlimited)
        html = bb.fetch_detail_anon_with_pdf(deal["source_url"], pdf_path)
        parsed = parse_bb_detail(html)

        # 2️⃣ FALL BACK TO AUTH ONLY IF NEEDED
        if not parsed["description"]:
            print("⚠️ No anon description — falling back to authenticated fetch")

            html = bb.fetch_listing_detail(deal)
            parsed = parse_bb_detail(html)

            content_hash = hashlib.sha256(html.encode()).hexdigest()

            bb.page.pdf(
                path=str(pdf_path),
                format="A4",
                print_background=True,
                margin={
                    "top": "20mm",
                    "bottom": "20mm",
                    "left": "15mm",
                    "right": "15mm",
                },
            )
        else:
            content_hash = hashlib.sha256(html.encode()).hexdigest()

        # 5. Persist — correct identity for BusinessBuyers
        repo.update_detail_fields_by_source(
            source="BusinessBuyers",
            source_listing_id=deal["source_listing_id"],
            fields={
                "description": parsed["description"],
                "asking_price": parsed["asking_price"],
                "asking_price_k": parsed["asking_price_k"],
                "extracted_json": json.dumps(parsed["facts"]),
                "content_hash": content_hash,
                "pdf_path": str(pdf_path),
            },
        )

        # 6. Mark detail complete (🔑 THIS WAS MISSING)
        repo.mark_detail_checked(
            deal_id,
            reason="bb_detail_enrichment",
        )

        print(f"✅ Enriched {deal_key}")
        print(f"   PDF: {pdf_path}")

    print("\n🏁 BusinessBuyers detail enrichment complete")


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    if __name__ == "__main__":
        limit_env = os.getenv("ENRICH_LIMIT")
        limit = None if limit_env in (None, "", "none") else int(limit_env)

        enrich_businessbuyers(limit=limit)
