"""
Daltons enrichment

Contract:
- Enrichment only (no import)
- Capture raw data only
- PDF + artifact creation
- Deduplicate against broker PRIMARY deals
- DRY_RUN supported
"""

import csv
import time
import random
from pathlib import Path
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

from src.persistence.repository import SQLiteRepository
from src.persistence.deal_artifacts import record_deal_artifact
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.utils.hash_utils import compute_content_hash, compute_file_hash
from src.sector_mappings.daltons import map_daltons_sector

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "Daltons"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/daltons_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DETAIL_WAIT_SELECTOR = "body"
SLEEP_BETWEEN = (3, 6)

DRY_RUN = 1 # os.getenv("DRY_RUN", "1") == "1"
HEADLESS = True

DALTONS_EXTRACTION_VERSION = "v1-detail"

CAPTURE_CSV = Path("/tmp/daltons_sector_raw_capture.csv")

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def extract_daltons_sector_raw(soup: BeautifulSoup) -> Optional[str]:
    crumbs = soup.select("ol.breadcrumb li a span, ol.breadcrumb li span")
    if not crumbs:
        return None

    parts = [c.get_text(strip=True) for c in crumbs]

    if parts and parts[0].lower() == "home":
        parts = parts[1:]

    if len(parts) >= 2:
        return " > ".join(parts)

    return None


def extract_description(soup: BeautifulSoup) -> Optional[str]:
    paras = soup.select(".item-description p, .property-description p")
    text = " ".join(
        p.get_text(" ", strip=True)
        for p in paras
        if len(p.get_text(strip=True)) > 30
    )
    return text if len(text) > 100 else None


def extract_location(soup: BeautifulSoup) -> Optional[str]:
    el = soup.select_one(".loc-urls-wrap")
    return el.get_text(" ", strip=True) if el else None


def is_lost_listing(soup: BeautifulSoup) -> bool:
    h1 = soup.select_one("h1")
    if h1 and "not found" in h1.get_text(strip=True).lower():
        return True
    return False


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def enrich_daltons(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")
    print(f"🏷️ Daltons enrichment starting | DRY_RUN={DRY_RUN}")

    repo = SQLiteRepository(DB_PATH)
    conn = repo.get_conn()

    deals = repo.fetch_deals_for_enrichment(source=SOURCE)
    if limit:
        deals = deals[:limit]

    print(f"🔍 {len(deals)} Daltons deals to enrich")
    if not deals:
        return

    captured_ids: set[str] = set()
    csv_file = None
    csv_writer = None

    if DRY_RUN:
        if CAPTURE_CSV.exists():
            with CAPTURE_CSV.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    captured_ids.add(row["source_listing_id"])

        csv_mode = "a" if CAPTURE_CSV.exists() else "w"
        csv_file = CAPTURE_CSV.open(csv_mode, newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(
            csv_file,
            fieldnames=["source_listing_id", "sector_raw"],
        )
        if csv_mode == "w":
            csv_writer.writeheader()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)

        try:
            for i, deal in enumerate(deals, start=1):
                row_id = deal["id"]
                url = deal["source_url"]
                listing_id = deal["source_listing_id"]
                if DRY_RUN and listing_id in captured_ids:
                    print("⏭ already captured, skipping")
                    continue

                print(f"\n➡️ [{i}/{len(deals)}] {listing_id}")
                print(url)

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
                    print("⚠️ Timeout")
                    context.close()
                    continue

                soup = BeautifulSoup(page.content(), "html.parser")

                if is_lost_listing(soup):
                    print("⚠️ Lost listing")
                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET status = 'Lost',
                                needs_detail_refresh = 0,
                                detail_fetched_at = CURRENT_TIMESTAMP,
                                last_updated = CURRENT_TIMESTAMP,
                                last_updated_source = 'AUTO'
                            WHERE id = ?
                            """,
                            (row_id,),
                        )
                        conn.commit()
                    context.close()
                    continue

                title = soup.select_one("h1")
                title = title.get_text(strip=True) if title else None

                description = extract_description(soup)
                sector_raw = extract_daltons_sector_raw(soup)
                location = extract_location(soup)

                if not title or not description:
                    print("⚠️ Incomplete content")
                    context.close()
                    continue

                content_hash = compute_content_hash(
                    title=title,
                    description=description,
                    location=location or "",
                )

                # ---------------- PDF ----------------
                pdf_path = PDF_ROOT / f"{listing_id}.pdf"

                page.add_style_tag(content="""
                header, footer, nav, button, iframe,
                .cookie-banner, .cta {
                    display: none !important;
                }
                """)

                page.wait_for_timeout(500)
                page.emulate_media(media="print")

                if DRY_RUN:
                    print("🔍 DRY RUN – PDF / Drive / DB skipped")
                    print("sector_raw:", sector_raw)

                    csv_writer.writerow({
                        "source_listing_id": listing_id,
                        "sector_raw": sector_raw,
                    })
                    csv_file.flush()
                    captured_ids.add(listing_id)

                    context.close()
                    continue

                page.pdf(
                    path=str(pdf_path),
                    format="A4",
                    print_background=True,
                )

                pdf_hash = compute_file_hash(pdf_path)

                # ---------------- Drive ----------------
                parent_folder_id = get_drive_parent_folder_id(
                    industry="Unclassified",
                    broker="Daltons",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=f"DAL-{listing_id}",
                    deal_title=title,
                )

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{listing_id}.pdf",
                    folder_id=deal_folder_id,
                )

                # ---------------- DB UPDATE ----------------
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        canonical_external_id = COALESCE(canonical_external_id, ?),
                        title = ?,
                        description = ?,
                        sector_raw = ?,
                        location = ?,
                        content_hash = ?,

                        drive_folder_id = ?,
                        drive_folder_url =
                          'https://drive.google.com/drive/folders/' || ?,

                        detail_fetched_at = ?,
                        needs_detail_refresh = 0,
                        last_updated = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                    """,
                    (
                        listing_id,
                        title,
                        description,
                        sector_raw,
                        location,
                        content_hash,
                        deal_folder_id,
                        deal_folder_id,
                        datetime.utcnow().isoformat(),
                        row_id,
                    ),
                )
                conn.commit()

                record_deal_artifact(
                    conn=conn,
                    source=SOURCE,
                    source_listing_id=listing_id,
                    deal_id=row_id,
                    artifact_type="pdf",
                    artifact_name=f"{listing_id}.pdf",
                    artifact_hash=pdf_hash,
                    drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                    drive_url=pdf_drive_url,
                    extraction_version=DALTONS_EXTRACTION_VERSION,
                    created_by="enrich_daltons.py",
                )

                pdf_path.unlink(missing_ok=True)

                print("✅ Enriched")
                context.close()
                time.sleep(random.uniform(*SLEEP_BETWEEN))

        finally:
            browser.close()
            conn.close()
            if csv_file:
                csv_file.close()

    print("🏁 Daltons enrichment complete")


if __name__ == "__main__":
    enrich_daltons()