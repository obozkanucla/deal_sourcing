from pathlib import Path
import time
import random
from typing import Optional

from playwright.sync_api import sync_playwright

from src.persistence.repository import SQLiteRepository
from src.persistence.deal_artifacts import record_deal_artifact
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.utils.hash_utils import compute_file_hash

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "Abercorn"
BROKER_NAME = "Abercorn"
ABERCORN_EXTRACTION_VERSION = "v1-listing+im"

PDF_ROOT = Path("/tmp/abercorn_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

SLEEP_BETWEEN = (1.5, 3.0)
DRY_RUN = False

repo = SQLiteRepository(Path("db/deals.sqlite"))

# -------------------------------------------------
# ENRICHMENT
# -------------------------------------------------

def enrich_abercorn(limit: Optional[int] = None) -> None:
    deals = repo.fetch_deals_for_enrichment(source=SOURCE)
    if limit:
        deals = deals[:limit]

    print(f"🔍 Found {len(deals)} Abercorn deals needing enrichment")
    if not deals:
        return

    conn = repo.get_conn()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        try:
            for i, deal in enumerate(deals, start=1):
                ref = deal["source_listing_id"]
                url = deal["source_url"]
                title = deal["title"] or ""
                industry = deal["industry"]

                print(f"\n➡️ [{i}/{len(deals)}] {ref}")
                print(url)

                if not url or url.endswith("/#"):
                    print("⚠️ Invalid listing URL — marking Lost")

                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET status = 'Lost',
                                lost_reason = 'Invalid source URL',
                                needs_detail_refresh = 0,
                                detail_fetched_at = CURRENT_TIMESTAMP,
                                last_updated = CURRENT_TIMESTAMP,
                                last_updated_source = 'AUTO'
                            WHERE id = ?
                            """,
                            (deal["id"],),
                        )
                        conn.commit()
                    continue

                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )

                try:
                    page = context.new_page()
                    page.goto(url, timeout=60_000, wait_until="domcontentloaded")

                    # -------------------------------------------------
                    # DRIVE FOLDER (AUTHORITATIVE)
                    # -------------------------------------------------
                    if DRY_RUN:
                        deal_folder_id = "DRY_RUN"
                    else:
                        parent_folder_id = get_drive_parent_folder_id(
                            industry=industry,
                            broker=BROKER_NAME,
                        )
                        deal_folder_id = find_or_create_deal_folder(
                            parent_folder_id=parent_folder_id,
                            deal_id=ref,
                            deal_title=title,
                        )

                        conn.execute(
                            """
                            UPDATE deals
                            SET drive_folder_id = ?,
                                drive_folder_url = 'https://drive.google.com/drive/folders/' || ?,
                                last_updated = CURRENT_TIMESTAMP,
                                last_updated_source = 'AUTO'
                            WHERE id = ?
                            """,
                            (deal_folder_id, deal_folder_id, deal["id"]),
                        )
                        conn.commit()

                    # -------------------------------------------------
                    # LISTING PAGE PDF
                    # -------------------------------------------------
                    listing_pdf_path = PDF_ROOT / f"{ref}-listing.pdf"

                    page.emulate_media(media="print")
                    page.pdf(
                        path=str(listing_pdf_path),
                        format="A4",
                        margin={"top": "15mm", "bottom": "15mm"},
                        print_background=True,
                    )

                    if listing_pdf_path.exists() and listing_pdf_path.stat().st_size > 10_000:
                        listing_hash = compute_file_hash(listing_pdf_path)

                        if not DRY_RUN:
                            drive_url = upload_pdf_to_drive(
                                local_path=listing_pdf_path,
                                filename=f"{ref}-listing.pdf",
                                folder_id=deal_folder_id,
                            )

                            record_deal_artifact(
                                conn=conn,
                                source=SOURCE,
                                source_listing_id=ref,
                                deal_id=deal["id"],
                                artifact_type="listing_pdf",
                                artifact_name=f"{ref}-listing.pdf",
                                artifact_hash=listing_hash,
                                drive_file_id=drive_url.split("/d/")[1].split("/")[0],
                                drive_url=drive_url,
                                extraction_version=ABERCORN_EXTRACTION_VERSION,
                                created_by="enrich_abercorn.py",
                            )

                        listing_pdf_path.unlink(missing_ok=True)

                    # -------------------------------------------------
                    # INFORMATION MEMORANDUM (IM)
                    # -------------------------------------------------
                    im_url = f"https://abercornbusinesssales.com/download-nda.php?id={ref}"
                    response = context.request.get(im_url, timeout=60_000)

                    if not response.ok:
                        print("⚠️ IM download failed")
                        continue

                    im_pdf_path = PDF_ROOT / f"{ref}.pdf"
                    im_pdf_path.write_bytes(response.body())

                    if im_pdf_path.stat().st_size < 10_000:
                        im_pdf_path.unlink(missing_ok=True)
                        continue

                    im_hash = compute_file_hash(im_pdf_path)

                    if not DRY_RUN:
                        im_drive_url = upload_pdf_to_drive(
                            local_path=im_pdf_path,
                            filename=f"{ref}.pdf",
                            folder_id=deal_folder_id,
                        )

                        record_deal_artifact(
                            conn=conn,
                            source=SOURCE,
                            source_listing_id=ref,
                            deal_id=deal["id"],
                            artifact_type="information_memorandum",
                            artifact_name=f"{ref}.pdf",
                            artifact_hash=im_hash,
                            drive_file_id=im_drive_url.split("/d/")[1].split("/")[0],
                            drive_url=im_drive_url,
                            extraction_version=ABERCORN_EXTRACTION_VERSION,
                            created_by="enrich_abercorn.py",
                        )

                        conn.execute(
                            """
                            UPDATE deals
                            SET pdf_drive_url = ?,
                                needs_detail_refresh = 0,
                                detail_fetched_at = CURRENT_TIMESTAMP,
                                last_updated = CURRENT_TIMESTAMP,
                                last_updated_source = 'AUTO'
                            WHERE id = ?
                            """,
                            (im_drive_url, deal["id"]),
                        )
                        conn.commit()

                    im_pdf_path.unlink(missing_ok=True)
                    print("✅ Listing + IM uploaded")

                    time.sleep(random.uniform(*SLEEP_BETWEEN))

                finally:
                    context.close()

        finally:
            browser.close()
            conn.close()

    print("\n🏁 Abercorn enrichment complete")


if __name__ == "__main__":
    enrich_abercorn()