# src/scripts/enrich_businesses4sale_generic.py
#
# BusinessesForSale – Generic enrichment
# IDENTICAL to Vault enrichment, except:
# - source = BusinessesForSale_Generic
# - MV ID optional (slug fallback)
# - DRY_RUN = True

import os
import time
import random
import sqlite3
import re
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

DRY_RUN = False

BASE_INDUSTRY = "Other"
BASE_SECTOR = "Miscellaneous"
BASE_CONFIDENCE = 0.2
BASE_REASON = "BusinessesForSale generic base assignment"

B4S_EXTRACTION_VERSION = "v1-generic"

B4S_LOST_PHRASES = [
    "oops! it looks like the page you were looking for doesn't exist",
    "page not found",
    "404",
    "no longer available",
]

HEADLESS = True # os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def is_b4s_lost(soup: BeautifulSoup) -> bool:
    # Explicit B4S dead-page patterns only

    # 1. Known dead-page heading
    h1 = soup.select_one("h1")
    if h1 and "oops" in h1.get_text(strip=True).lower():
        return True

    # 2. Missing core listing blocks (hard fail)
    if not soup.select_one("#listing-wrap"):
        return True

    if not soup.select_one(".listing-title"):
        return True

    return False

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None

def extract_any_description(soup: BeautifulSoup) -> Optional[str]:
    # 1. Bullet-style listings (very common on B4S)
    bullets = soup.select("div.listing-section-content li")
    if bullets:
        text = " ".join(
            li.get_text(" ", strip=True)
            for li in bullets
            if len(li.get_text(strip=True)) > 10
        )
        if len(text) > 80:
            return text

    # 2. Paragraph descriptions
    paras = soup.select("div.listing-section-content p")
    if paras:
        text = " ".join(
            p.get_text(" ", strip=True)
            for p in paras
            if len(p.get_text(strip=True)) > 20
        )
        if len(text) > 80:
            return text

    # 3. Weak fallbacks
    for sel in ["article", "section"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if len(txt) > 80:
                return txt

    return None

def extract_b4s_financials(soup: BeautifulSoup) -> dict:
    facts = {}

    for dl in soup.select("div.financials-group dl, div.overview-details dl"):
        dt = dl.select_one("dt")
        dd = dl.select_one("dd")
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
    """
    Generic pages usually do NOT have MV IDs.
    Use listing-id span if present, else slug.
    """
    el = soup.select_one("#listing-id")
    if el:
        return el.get_text(strip=True)
    return fallback_slug

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

    print(f"🔍 {len(deals)} {SOURCE} deals to enrich")
    if not deals:
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)

        try:
            for i, deal in enumerate(deals, start=1):
                if time.time() - START > MAX_RUNTIME:
                    print("⏱ stopping early to respect CI limits")
                    break
                row_id = deal["id"]
                url = deal["source_url"]
                slug = deal["source_listing_id"]

                print(f"\n➡️ [{i}/{len(deals)}] {slug}")
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
                    print("⚠️ Page timeout")
                    context.close()
                    continue

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                if is_b4s_lost(soup):
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

                title = text_or_none(soup.select_one("h1"))
                description = extract_any_description(soup)
                location = text_or_none(soup.select_one("#address"))

                if not title or not description:
                    print("⚠️ Incomplete content")
                    context.close()
                    continue

                canonical_external_id = extract_listing_id(soup, slug)
                financials = extract_b4s_financials(soup)

                content_hash = compute_content_hash(
                    title=title,
                    description=description,
                    location=location or "",
                )

                # ---------------- PDF ----------------
                pdf_path = PDF_ROOT / f"{canonical_external_id}.pdf"

                page.add_style_tag(content="""
                body * { visibility: hidden !important; }
                #main-listing-content, #main-listing-content * {
                    visibility: visible !important;
                }
                header, footer, nav, button,
                .cookie-banner, #onetrust-consent-sdk,
                .cta, aside, iframe {
                    display: none !important;
                }
                """)

                page.wait_for_timeout(500)
                page.emulate_media(media="print")

                if not DRY_RUN:
                    page.pdf(
                        path=str(pdf_path),
                        format="A4",
                        margin={"top": "15mm", "bottom": "15mm",
                                "left": "15mm", "right": "15mm"},
                        print_background=True,
                    )
                    print(f"📄 PDF created: {pdf_path}")
                if DRY_RUN:
                    print("🔍 DRY RUN – PDF + Drive + DB skipped")
                    context.close()
                    continue

                # ---------------- Drive ----------------
                parent_folder_id = get_drive_parent_folder_id(
                    industry=BASE_INDUSTRY,
                    broker="BusinessesForSale",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=f"B4S-GEN-{canonical_external_id}",
                    deal_title=title,
                )

                pdf_hash = compute_file_hash(pdf_path)

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{canonical_external_id}.pdf",
                    folder_id=deal_folder_id,
                )
                conn.execute(
                    """
                    UPDATE deals
                    SET drive_folder_id     = ?,
                        drive_folder_url    = 'https://drive.google.com/drive/folders/' || ?,
                        last_updated        = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                    """,
                    (deal_folder_id, deal_folder_id, deal["id"]),
                )
                conn.commit()

                record_deal_artifact(
                    conn=conn,
                    source=SOURCE,
                    source_listing_id=canonical_external_id,
                    deal_id=row_id,
                    artifact_type="pdf",
                    artifact_name=f"{canonical_external_id}.pdf",
                    artifact_hash=pdf_hash,
                    drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                    drive_url=pdf_drive_url,
                    extraction_version=B4S_EXTRACTION_VERSION,
                    created_by="enrich_businesses4sale_generic.py",
                )

                pdf_path.unlink(missing_ok=True)

                # ---------------- DB UPDATE ----------------
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        canonical_external_id = COALESCE(canonical_external_id, ?),
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
                        canonical_external_id,
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
                        datetime.today().isoformat(),
                        row_id,
                    ),
                )
                conn.commit()

                print("✅ Enriched")
                context.close()
                time.sleep(random.uniform(*SLEEP_BETWEEN))

        finally:
            browser.close()
            conn.close()

    print("\n🏁 BusinessesForSale_Generic enrichment complete")


if __name__ == "__main__":
    enrich_businesses4sale_generic()