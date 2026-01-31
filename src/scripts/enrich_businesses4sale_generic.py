# src/scripts/enrich_businesses4sale_generic.py
#
# BusinessesForSale – Generic enrichment
# Identity-safe version (BSR-aligned)

# TODO [DATA CLEANUP]:
# BusinessesForSale_Generic contains legacy rows where:
#   - source_listing_id is numeric (historic import behavior)
#   - canonical_external_id is NULL
#
# These rows are SAFE to process but represent mixed identity semantics.
#
# Future cleanup (one-off, offline):
# - Preserve numeric ID as canonical_external_id
# - Restore slug-based source_listing_id from source_url
# - Ensure no Drive folder duplication
#
# Enrichment logic MUST NOT mutate source_listing_id.

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
from src.utils.financial_normalization import _normalize_money_to_k, _normalize_pct

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "BusinessesForSale_Generic"
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/businesses4sale_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DETAIL_WAIT_SELECTOR = "body"
SLEEP_BETWEEN = (3, 6)

HEADLESS = True
DRY_RUN = False

BASE_INDUSTRY = "Other"
BASE_SECTOR = "Miscellaneous"
BASE_CONFIDENCE = 0.2
BASE_REASON = "BusinessesForSale generic base assignment"

B4S_EXTRACTION_VERSION = "v1-generic"

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def is_b4s_lost(soup: BeautifulSoup) -> bool:
    h1 = soup.select_one("h1")
    if h1 and "oops" in h1.get_text(strip=True).lower():
        return True
    if not soup.select_one("#listing-wrap"):
        return True
    if not soup.select_one(".listing-title"):
        return True
    return False


def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None


def extract_any_description(soup: BeautifulSoup) -> Optional[str]:
    bullets = soup.select("div.listing-section-content li")
    if bullets:
        text = " ".join(li.get_text(" ", strip=True) for li in bullets)
        if len(text) > 80:
            return text

    paras = soup.select("div.listing-section-content p")
    if paras:
        text = " ".join(p.get_text(" ", strip=True) for p in paras)
        if len(text) > 80:
            return text

    return None


def extract_b4s_financials(soup: BeautifulSoup) -> dict:
    facts = {}
    for dl in soup.select("div.financials-group dl, div.overview-details dl"):
        dt, dd = dl.select_one("dt"), dl.select_one("dd")
        if not dt or not dd:
            continue
        label = dt.get_text(strip=True).lower()
        value = dd.get_text(strip=True)

        if "turnover" in label:
            v = _normalize_money_to_k(value)
            if v is not None:
                facts["revenue_k"] = v
        elif "profit" in label or "ebitda" in label:
            v = _normalize_money_to_k(value)
            if v is not None:
                facts["ebitda_k"] = v
        elif "margin" in label:
            v = _normalize_pct(value)
            if v is not None:
                facts["profit_margin_pct"] = v

    return facts


def extract_listing_id(soup: BeautifulSoup, fallback_slug: str) -> str:
    el = soup.select_one("#listing-id")
    return el.get_text(strip=True) if el else fallback_slug


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def enrich_businesses4sale_generic(limit: Optional[int] = None) -> None:
    START = time.time()
    MAX_RUNTIME = 40 * 60

    print(f"📀 SQLite DB path: {DB_PATH}")

    repo = SQLiteRepository(DB_PATH)
    conn = repo.get_conn()

    deals = repo.fetch_deals_for_enrichment(source=SOURCE)
    if limit:
        deals = deals[:limit]

    print(f"🔍 Deals fetched for enrichment: {len(deals)}")

    if not deals:
        print("✅ Nothing to enrich")
        return

    enriched = 0
    lost = 0
    skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)

        try:
            for i, deal in enumerate(deals, start=1):
                if time.time() - START > MAX_RUNTIME:
                    print("⏱ stopping early (CI limit)")
                    break

                row_id = deal["id"]
                url = deal["source_url"]
                import_slug = deal["source_listing_id"]

                print(f"\n➡️ [{i}/{len(deals)}] {import_slug}")
                print(url)

                context = browser.new_context()
                page = context.new_page()

                try:
                    page.goto(url, timeout=60_000)
                    page.wait_for_selector(DETAIL_WAIT_SELECTOR, timeout=20_000)
                except TimeoutError:
                    print("⚠️ Page timeout")
                    skipped += 1
                    context.close()
                    continue

                soup = BeautifulSoup(page.content(), "html.parser")

                if is_b4s_lost(soup):
                    print("⚠️ Lost listing")
                    lost += 1
                    conn.execute(
                        """
                        UPDATE deals
                        SET status='Lost',
                            needs_detail_refresh=0,
                            last_updated=CURRENT_TIMESTAMP,
                            last_updated_source='AUTO'
                        WHERE id=?
                        """,
                        (row_id,),
                    )
                    conn.commit()
                    context.close()
                    continue

                title = text_or_none(soup.select_one("h1"))
                description = extract_any_description(soup)
                location = text_or_none(soup.select_one("#address"))

                if not title or not description:
                    print("⚠️ Incomplete content")
                    skipped += 1
                    context.close()
                    continue

                canonical_id = extract_listing_id(soup, import_slug)
                financials = extract_b4s_financials(soup)

                content_hash = compute_content_hash(
                    title=title,
                    description=description,
                    location=location or "",
                )

                pdf_path = PDF_ROOT / f"{canonical_id}.pdf"

                page.emulate_media(media="print")
                page.pdf(path=str(pdf_path), format="A4", print_background=True)

                parent_folder_id = get_drive_parent_folder_id(
                    industry=BASE_INDUSTRY,
                    broker="BusinessesForSale",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=f"B4S-GEN-{canonical_id}",
                    deal_title=title,
                )

                pdf_hash = compute_file_hash(pdf_path)
                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{canonical_id}.pdf",
                    folder_id=deal_folder_id,
                )

                record_deal_artifact(
                    conn=conn,
                    source=SOURCE,
                    source_listing_id=canonical_id,
                    deal_id=row_id,
                    artifact_type="pdf",
                    artifact_name=f"{canonical_id}.pdf",
                    artifact_hash=pdf_hash,
                    drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                    drive_url=pdf_drive_url,
                    extraction_version=B4S_EXTRACTION_VERSION,
                    created_by="enrich_businesses4sale_generic.py",
                )

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        canonical_external_id = ?,
                        title = ?,
                        description = ?,
                        location = ?,
                        content_hash = ?,

                        revenue_k = COALESCE(?, revenue_k),
                        ebitda_k = COALESCE(?, ebitda_k),
                        profit_margin_pct = COALESCE(?, profit_margin_pct),

                        industry = ?,
                        sector = ?,
                        sector_source = 'unclassified',
                        sector_inference_confidence = ?,
                        sector_inference_reason = ?,

                        pdf_drive_url = ?,
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
                        canonical_id,
                        title,
                        description,
                        location,
                        content_hash,

                        financials.get("revenue_k"),
                        financials.get("ebitda_k"),
                        financials.get("profit_margin_pct"),

                        BASE_INDUSTRY,
                        BASE_SECTOR,
                        BASE_CONFIDENCE,
                        BASE_REASON,

                        pdf_drive_url,
                        deal_folder_id,
                        deal_folder_id,
                        datetime.utcnow().isoformat(),
                        row_id,
                    ),
                )
                conn.commit()

                enriched += 1
                print("✅ Enriched")

                pdf_path.unlink(missing_ok=True)
                context.close()
                time.sleep(random.uniform(*SLEEP_BETWEEN))

        finally:
            browser.close()
            conn.close()

    print(
        f"\n🏁 BusinessesForSale_Generic enrichment complete | "
        f"Enriched: {enriched}, Lost: {lost}, Skipped: {skipped}"
    )


if __name__ == "__main__":
    enrich_businesses4sale_generic()