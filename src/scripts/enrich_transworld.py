from pathlib import Path
from datetime import datetime
import time
import random
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
from src.utils.hash_utils import compute_file_hash
from src.utils.financial_normalization import _normalize_money_to_k
from src.sector_mappings.transworld import map_transworld_category
from src.domain.industries import assert_valid_industry

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "transworld_uk"
BROKER_NAME = "TransworldUK"
TRANSWORLD_EXTRACTION_VERSION = "v1"

PDF_ROOT = Path("/tmp/transworld_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DETAIL_WAIT_SELECTOR = "div.description-wrapper"
SLEEP_BETWEEN = (2.0, 4.0)

DRY_RUN = False

repo = SQLiteRepository(Path("db/deals.sqlite"))

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None


def extract_listing_number(soup: BeautifulSoup) -> Optional[str]:
    for li in soup.select("ul.listing-details-list li"):
        title = li.select_one("span.title")
        value = li.select_one("span.value strong")
        if title and value:
            if title.get_text(strip=True).lower() == "listing number:":
                return value.get_text(strip=True)
    return None


def extract_listing_details(soup: BeautifulSoup) -> dict:
    out = {}

    for li in soup.select("ul.listing-details-list li"):
        title_el = li.select_one("span.title")
        value_el = li.select_one("span.value strong")

        if not title_el or not value_el:
            continue

        label = title_el.get_text(strip=True).lower()
        value = value_el.get_text(strip=True)

        if label.startswith("location"):
            out["location"] = value
        elif label.startswith("price"):
            out["asking_price_k"] = _normalize_money_to_k(value)
        elif "sellers discretionary earnings" in label:
            out["ebitda_k"] = _normalize_money_to_k(value)
        elif label.startswith("category"):
            out["sector_raw"] = value
        elif label.startswith("reason for selling"):
            out["notes"] = f"Reason for selling: {value}"

    return out


# -------------------------------------------------
# ENRICHMENT
# -------------------------------------------------

def enrich_transworld(limit: Optional[int] = None) -> None:
    deals = repo.fetch_deals_for_enrichment(source=SOURCE)
    if limit:
        deals = deals[:limit]

    print(f"🔍 Found {len(deals)} Transworld deals needing enrichment")
    if not deals:
        return

    conn = repo.get_conn()   # ✅ SINGLE CONNECTION

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        try:
            for i, deal in enumerate(deals, start=1):
                slug = deal["source_listing_id"]
                url = deal["source_url"]
                title = deal["title"] or ""

                print(f"\n➡️ [{i}/{len(deals)}] {slug}")
                print(url)

                # -------------------------------
                # SOLD → LOST
                # -------------------------------
                current_identifier = deal["source_listing_id"] or ""

                if "sold" in title.lower() or "/sold" in url.lower():
                    canonical_id = (
                        current_identifier
                        if current_identifier.startswith("TW-SOLD-")
                        else f"TW-SOLD-{current_identifier}"
                    )
                    print("⚠️ Marked SOLD — canonicalising + setting Lost")
                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET canonical_external_id    = COALESCE(canonical_external_id, ?),
                                status               = 'Lost',
                                lost_reason          = 'Marked SOLD in listing',
                                needs_detail_refresh = 0,
                                detail_fetched_at    = ?,
                                last_updated         = CURRENT_TIMESTAMP,
                                last_updated_source  = 'AUTO'
                            WHERE id = ?
                            """,
                            (
                                canonical_id,
                                datetime.today().isoformat(),
                                deal["id"],
                            ),
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

                    try:
                        page.wait_for_selector(DETAIL_WAIT_SELECTOR, timeout=20_000)
                    except TimeoutError:
                        print("⚠️ Description wrapper missing")

                    soup = BeautifulSoup(page.content(), "html.parser")

                    raw_listing_number = extract_listing_number(soup)

                    # -------------------------------
                    # HARD LOST: redirect / no detail
                    # -------------------------------
                    if not raw_listing_number:
                        print("⚠️ Listing number missing — marking Lost")
                        if not DRY_RUN:
                            conn.execute(
                                """
                                UPDATE deals
                                SET status = 'Lost',
                                    lost_reason = 'Redirected to listings index',
                                    needs_detail_refresh = 0,
                                    detail_fetched_at = ?,
                                    last_updated = CURRENT_TIMESTAMP,
                                    last_updated_source = 'AUTO'
                                WHERE id = ?
                                """,
                                (
                                    datetime.today().isoformat(),
                                    deal["id"]
                                ),
                            )
                            conn.commit()
                        continue

                    listing_number = f"TW-{raw_listing_number}"

                    existing = conn.execute(
                        """
                        SELECT id
                        FROM deals
                        WHERE source = ?
                          AND canonical_external_id = ?
                          AND id != ?
                        """,
                        (SOURCE, listing_number, deal["id"]),
                    ).fetchone()

                    if existing:
                        print("⚠️ Duplicate Transworld listing number — skipping override")
                        conn.execute(
                            """
                            UPDATE deals
                            SET
                                needs_detail_refresh = 0,
                                detail_fetched_at    = CURRENT_TIMESTAMP,
                                detail_fetch_reason  = 'canonicalised_elsewhere',
                                last_updated         = CURRENT_TIMESTAMP,
                                last_updated_source  = 'AUTO'
                            WHERE id = ?;
                            """,
                            (deal["id"],),
                        )
                        conn.commit()
                        continue

                    facts = extract_listing_details(soup)
                    description = text_or_none(
                        soup.select_one("div.description-wrapper p")
                    )

                    mapping = map_transworld_category(
                        sector_raw=facts.get("sector_raw"),
                        title=title,
                    )

                    industry = mapping["industry"] or "Other"
                    sector = mapping["sector"]

                    assert_valid_industry(industry)

                    fetched_at = datetime.today().isoformat()

                    if DRY_RUN:
                        print("🔍 DRY RUN")
                        continue

                    # -------------------------------
                    # PDF
                    # -------------------------------
                    pdf_path = PDF_ROOT / f"{listing_number}.pdf"
                    page.emulate_media(media="print")
                    page.pdf(
                        path=str(pdf_path),
                        format="A4",
                        margin={"top": "15mm", "bottom": "15mm"},
                        print_background=True,
                    )

                    if not pdf_path.exists() or pdf_path.stat().st_size < 10_000:
                        pdf_path.unlink(missing_ok=True)
                        continue

                    parent_folder_id = get_drive_parent_folder_id(
                        industry=industry,
                        broker=BROKER_NAME,
                    )

                    deal_folder_id = find_or_create_deal_folder(
                        parent_folder_id=parent_folder_id,
                        deal_id=listing_number,
                        deal_title=title,
                    )

                    pdf_hash = compute_file_hash(pdf_path)

                    pdf_drive_url = upload_pdf_to_drive(
                        local_path=str(pdf_path),
                        filename=f"{listing_number}.pdf",
                        folder_id=deal_folder_id,
                    )

                    record_deal_artifact(
                        conn=conn,
                        source=SOURCE,
                        source_listing_id=listing_number,
                        deal_id=deal["id"],
                        artifact_type="pdf",
                        artifact_name=f"{listing_number}.pdf",
                        artifact_hash=pdf_hash,
                        drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                        drive_url=pdf_drive_url,
                        extraction_version=TRANSWORLD_EXTRACTION_VERSION,
                        created_by="enrich_transworld.py",
                    )

                    pdf_path.unlink(missing_ok=True)

                    # -------------------------------
                    # FINAL UPDATE
                    # -------------------------------
                    conn.execute(
                        """
                        UPDATE deals
                        SET canonical_external_id = COALESCE(canonical_external_id, ?),
                            description = ?,
                            location = ?,
                            sector_raw = ?,
                            industry = ?,
                            sector = ?,
                            sector_source = 'broker',
                            sector_inference_confidence = ?,
                            sector_inference_reason = ?,
                            asking_price_k = ?,
                            ebitda_k = ?,
                            notes = ?,
                            pdf_drive_url = ?,
                            drive_folder_id = ?,
                            drive_folder_url = 'https://drive.google.com/drive/folders/' || ?,
                            detail_fetched_at = ?,
                            needs_detail_refresh = 0,
                            last_updated = CURRENT_TIMESTAMP,
                            last_updated_source = 'AUTO'
                        WHERE id = ?
                        """,
                        (
                            listing_number,
                            description,
                            facts.get("location"),
                            facts.get("sector_raw"),
                            industry,
                            sector,
                            mapping["confidence"],
                            mapping["reason"],
                            facts.get("asking_price_k"),
                            facts.get("ebitda_k"),
                            facts.get("notes"),
                            pdf_drive_url,
                            deal_folder_id,
                            fetched_at,
                            deal["id"]
                        ),
                    )
                    conn.commit()

                    print("✅ Enriched + uploaded")
                    time.sleep(random.uniform(*SLEEP_BETWEEN))

                finally:
                    context.close()

        finally:
            browser.close()
            conn.close()

    print("\n🏁 Transworld enrichment complete")


if __name__ == "__main__":
    enrich_transworld()