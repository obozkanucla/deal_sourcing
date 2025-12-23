import json
from pathlib import Path
from datetime import datetime


from src.persistence.repository import SQLiteRepository
from src.utils.pdf_playwright import html_to_pdf   # Playwright-based
from src.brokers.businessbuyers_detail import scrape_bb_detail
from src.integrations.drive_folders import BROKER_FOLDERS

PDF_TMP_DIR = Path("/tmp/bb_pdfs")
PDF_TMP_DIR.mkdir(exist_ok=True)

MAX_PLAYWRIGHT_PER_RUN = 3     # 🔒 HARD LIMIT
DRIVE_FOLDER_ID = "SHARED_DRIVE_FOLDER_ID"

from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

def enrich_businessbuyers(limit=1):
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    deals = repo.fetch_deals_needing_details(limit)

    print(f"🔍 Checking {len(deals)} deals")

    for deal in deals:
        parent_folder_id = BROKER_FOLDERS.get(
            (deal["source"], deal["sector"])
        )

        if not parent_folder_id:
            raise ValueError(
                f"No Drive folder mapping for {deal['source']} / {deal['sector']}"
            )
        deal_id = deal["deal_id"]
        print(f"➡️ {deal_id}")

        print("🔥 Running full enrichment")

        # 1. Scrape detail
        data = scrape_bb_detail(deal["source_url"])

        # 2. Create / find folder
        folder_id = find_or_create_deal_folder(
            parent_folder_id=parent_folder_id,
            deal_id=deal['source_listing_id'],
        )

        # 3. Generate PDF path and file
        local_pdf_path = PDF_TMP_DIR / f"{deal_id}.pdf"
        html_to_pdf(
            html=data["raw_html"],
            output_path=local_pdf_path,
        )

        # 4. Upload PDF INTO THAT FOLDER
        pdf_url = upload_pdf_to_drive(
            local_path=local_pdf_path,
            filename=local_pdf_path.name,
            folder_id=folder_id
        )

        # 5. Persist everything
        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

        repo.update_detail_fields(
            deal["deal_id"],
            {
                "drive_folder_id": folder_id,
                "drive_folder_url": folder_url,
                "content_hash": data["content_hash"],
            }
        )

        print(f"✅ Enriched {deal_id}")

if __name__ == "__main__":
    enrich_businessbuyers(limit=1)