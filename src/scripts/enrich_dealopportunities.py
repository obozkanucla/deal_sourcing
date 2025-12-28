from pathlib import Path
import hashlib
import json
from bs4 import BeautifulSoup
import os

from src.persistence.repository import SQLiteRepository
from src.brokers.dealopportunities_client import DealOpportunitiesClient

# =========================
# CONFIG
# =========================

TMP_PDF_DIR = Path("/tmp/do_pdfs")
TMP_PDF_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# PARSER
# =========================

def parse_do_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # --- primary description ---
    desc_el = soup.select_one(
        ".opportunity-description, .content, article"
    )

    description = None
    if desc_el:
        description = desc_el.get_text("\n", strip=True)

    # --- fallback: legacy table layout ---
    if not description:
        table_td = soup.select_one("table td[valign='top']")
        if table_td:
            description = table_td.get_text("\n", strip=True)

    # --- structured facts ---
    facts = {}
    for dt in soup.select("dl dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        value = dd.get_text(strip=True) if dd else None

        if not value:
            continue

        if "sector" in label:
            facts["sector"] = value
        elif "region" in label:
            facts["region"] = value
        elif "turnover" in label:
            facts["turnover"] = value
        elif "added" in label:
            facts["added"] = value
        elif "reference" in label:
            facts["reference"] = value

    return {
        "description": description,
        "facts": facts,
    }

# =========================
# ENRICHMENT
# =========================

def enrich_dealopportunities(limit=5):
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_deals_needing_details_for_source(
        source="DealOpportunities",
        limit=limit if limit is not None else 10_000,
    )

    if not deals:
        print("✅ No DealOpportunities deals need enrichment — exiting")
        return

    print(f"🔍 Found {len(deals)} DealOpportunities deals needing details")

    client = DealOpportunitiesClient()
    client.start()

    try:
        for deal in deals:
            deal_key = deal["source_listing_id"]
            url = deal["source_url"]

            print(f"\n➡️ Enriching {deal_key}")
            print(url)

            pdf_path = TMP_PDF_DIR / f"{deal_key}.pdf"

            # 1. Fetch HTML + PDF (Playwright)
            html = client.fetch_listing_detail_and_pdf(
                url=url,
                pdf_path=pdf_path,
            )

            print("\n--- HTML sanity check ---")
            print(html[:1500])
            print("\n--- END HTML ---\n")

            # 2. Parse
            parsed = parse_do_detail(html)

            print("\n--- PARSER OUTPUT ---")
            print("Description length:", len(parsed["description"] or ""))
            print(parsed["description"][:500] if parsed["description"] else "❌ NO DESCRIPTION")
            print("Facts:", parsed["facts"])
            print("--- END PARSER OUTPUT ---\n")

            # 3. Hash
            content_hash = hashlib.sha256(html.encode()).hexdigest()

            # 4. Persist
            repo.update_detail_fields_by_source(
                source="DealOpportunities",
                source_listing_id=deal["source_listing_id"],  # D131xx
                fields={
                    "description": parsed["description"],
                    "extracted_json": json.dumps(parsed["facts"]),
                    "content_hash": content_hash,
                    "pdf_path": str(pdf_path),
                },
            )

            print(f"✅ Enriched {deal_key}")
            print(f"   PDF: {pdf_path}")

    finally:
        client.stop()
        print("\n🏁 DealOpportunities detail enrichment complete")


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    limit_env = os.getenv("ENRICH_LIMIT")
    limit = None if limit_env in (None, "", "none") else int(limit_env)

    enrich_dealopportunities(limit=limit)