import json
from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.brokers.businessbuyers_client import BusinessBuyersClient
from src.integrations.drive_folders import BROKER_FOLDERS
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.config import BB_USERNAME, BB_PASSWORD


PDF_TMP_DIR = Path("/tmp/bb_pdfs")
PDF_TMP_DIR.mkdir(parents=True, exist_ok=True)


def enrich_businessbuyers(limit: int = 1):
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_deals_needing_details(limit)
    print(f"🔍 Found {len(deals)} BusinessBuyers deals needing details")

    if not deals:
        return

    # --------------------------------------------------
    # Start ONE Playwright session
    # --------------------------------------------------
    bb = BusinessBuyersClient(
        username=BB_USERNAME,
        password=BB_PASSWORD,
        click_budget=None,  # not enforced for now
    )

    bb.login()

    for deal in deals:
        deal_id = deal["deal_id"] or deal["source_listing_id"]
        url = deal["source_url"]

        print(f"\n➡️ Enriching {deal_id}")
        print(url)

        # --------------------------------------------------
        # Drive folder resolution
        # --------------------------------------------------
        parent_folder_id = BROKER_FOLDERS.get(
            (deal["source"], deal["sector"])
        )

        if not parent_folder_id:
            raise ValueError(
                f"No Drive folder mapping for {deal['source']} / {deal['sector']}"
            )

        folder_id = find_or_create_deal_folder(
            parent_folder_id=parent_folder_id,
            deal_id=deal["source_listing_id"],
        )

        # --------------------------------------------------
        # Navigate to detail page
        # --------------------------------------------------
        bb.page.goto(url)
        bb.page.wait_for_load_state("networkidle")
        bb.ensure_cookies_cleared()

        raw_html = bb.page.content()

        # --------------------------------------------------
        # Generate PDF from SAME page
        # --------------------------------------------------
        local_pdf_path = PDF_TMP_DIR / f"{deal_id}.pdf"

        bb.page.pdf(
            path=str(local_pdf_path),
            format="A4",
            print_background=True,
            margin={
                "top": "20mm",
                "bottom": "20mm",
                "left": "15mm",
                "right": "15mm",
            },
        )

        # --------------------------------------------------
        # Upload PDF
        # --------------------------------------------------
        pdf_url = upload_pdf_to_drive(
            local_path=local_pdf_path,
            filename=local_pdf_path.name,
            folder_id=folder_id,
        )

        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

        # --------------------------------------------------
        # Persist to SQLite
        # --------------------------------------------------
        repo.update_detail_fields(
            deal["deal_id"],
            {
                "raw_html": raw_html,
                "pdf_path": str(local_pdf_path),
                "pdf_drive_url": pdf_url,
                "drive_folder_id": folder_id,
                "drive_folder_url": folder_url,
                "detail_fetched_at": datetime.utcnow().isoformat(),
            },
        )

        print(f"✅ Enriched {deal_id}")

    print("\n✅ BusinessBuyers detail enrichment complete")


if __name__ == "__main__":
    enrich_businessbuyers(limit=1)