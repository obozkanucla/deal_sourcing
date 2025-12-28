import json
from pathlib import Path

from src.persistence.repository import SQLiteRepository
from src._to_delete.businessbuyers_detailx import scrape_bb_detail
from src.utils.pdf_playwright import html_to_pdf

TMP_PDF_DIR = Path("/tmp/deals_pdfs/BusinessBuyers")
TMP_PDF_DIR.mkdir(parents=True, exist_ok=True)

def enrich_businessbuyers_detail(limit=1):
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_deals_needing_details(limit)
    print(f"🔍 Found {len(deals)} BB deals needing details")

    for deal in deals:
        deal_id = deal["deal_id"]
        url = deal["source_url"]

        print(f"\n➡️ Fetching detail for {deal_id}")
        print(url)

        # 1. Fetch HTML + parsed data
        data = scrape_bb_detail(url)

        # 2. Generate PDF from HTML
        pdf_path = TMP_PDF_DIR / f"{deal_id}.pdf"
        html_to_pdf(
            html=data["raw_html"],
            output_path=pdf_path,
        )

        # 3. Persist to SQLite
        repo.update_detail_fields(
            deal_id,
            {
                "description": data.get("description"),
                "extracted_json": json.dumps(data.get("facts")),
                "content_hash": data["content_hash"],
                "pdf_path": str(pdf_path),
            }
        )

        print(f"✅ Enriched {deal_id}")
        print(f"   PDF: {pdf_path}")

if __name__ == "__main__":
    enrich_businessbuyers_detail(limit=1)