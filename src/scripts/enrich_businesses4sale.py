import sqlite3
from pathlib import Path
from datetime import datetime
import os
import re
import hashlib
import time
import random
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

from src.persistence.repository import SQLiteRepository
from src.enrichment.financial_extractor import extract_financial_metrics
from src.persistence.deal_artifacts import record_deal_artifact
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.utils.financial_normalization import _normalize_money_to_k, _normalize_pct, normalize_from_description
from src.utils.hash_utils import compute_file_hash

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/businesses4sale_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DETAIL_WAIT_SELECTOR = "#hero, div.teaser-content"
SLEEP_BETWEEN = (3, 6)
DRY_RUN = False

# Base classification (intentional for B4S)
BASE_INDUSTRY = "Other"
BASE_SECTOR = "Miscellaneous"
BASE_CONFIDENCE = 0.2
BASE_REASON = "BusinessesForSale base assignment"
B4S_LOST_PHRASES = [
    "oops! it looks like the page you were looking for doesn't exist",
    "page not found",
    "404",
    "no longer available",
]

B4S_EXTRACTION_VERSION = 'v1'
# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def is_b4s_lost(html: str | None) -> bool:
    if not html:
        return False
    text = html.lower()
    return any(p in text for p in B4S_LOST_PHRASES)

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None


def extract_teaser_field(soup, heading: str) -> Optional[str]:
    for field in soup.select("div.teaser-field"):
        h3 = field.select_one("h3")
        if h3 and h3.get_text(strip=True).lower() == heading.lower():
            return text_or_none(field)
    return None


def extract_mv_id(soup: BeautifulSoup) -> Optional[str]:
    p = soup.select_one("div.teaser-ref p")
    if not p:
        return None
    m = re.search(r"\b(MV\d+)\b", p.get_text(strip=True))
    return m.group(1) if m else None

def find_existing_mv_owner(conn, mv_id: str, current_row_id: int) -> Optional[int]:
    row = conn.execute(
        """
        SELECT id
        FROM deals
        WHERE source = 'BusinessesForSale'
          AND source_listing_id = ?
          AND id != ?
        """,
        (mv_id, current_row_id),
    ).fetchone()
    return row["id"] if row else None

def extract_b4s_financials(soup: BeautifulSoup) -> dict:
    facts = {}

    for dl in soup.select("div.financials-group dl"):
        dt = dl.select_one("dt")
        dd = dl.select_one("dd")
        if not dt or not dd:
            continue

        label = dt.get_text(strip=True).lower()
        value = dd.get_text(strip=True)

        if "turnover" in label:
            v = _normalize_money_to_k(value)
            if v is not None:
                facts["turnover"] = {
                    "value_k": v,
                    "unit": "k",
                    "declared_as": value,
                }

        elif "ebitda" in label:
            v = _normalize_money_to_k(value)
            if v is not None:
                facts["ebitda"] = {
                    "value_k": v,
                    "unit": "k",
                    "declared_as": value,
                }

        elif "profitability" in label:
            v = _normalize_pct(value)
            if v is not None:
                facts["profit_margin"] = {
                    "pct": v,
                    "declared_as": value,
                }

        elif "growth" in label:
            v = _normalize_pct(value)
            if v is not None:
                facts["revenue_growth"] = {
                    "pct": v,
                    "declared_as": value,
                }

        elif "leverage" in label:
            v = _normalize_pct(value)
            if v is not None:
                facts["leverage"] = {
                    "pct": v,
                    "declared_as": value,
                }

    return facts

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def enrich_businesses4sale(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")

    repo = SQLiteRepository(DB_PATH)
    conn = repo.get_conn()

    deals = repo.fetch_deals_for_enrichment(
        source="BusinessesForSale",
    )

    if limit:
        deals = deals[:limit]

    print(f"🔍 Found {len(deals)} BusinessesForSale deals needing enrichment")
    if not deals:
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
        )

        try:
            for i, deal in enumerate(deals, start=1):
                row_id = deal["id"]
                url = deal["source_url"]

                print(f"\n➡️ [{i}/{len(deals)}] {deal['source_listing_id']}")
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
                    print("⚠️ Listing unavailable — marking Lost")
                    if not DRY_RUN:
                        conn.execute(
                            """
                                UPDATE deals
                                SET status = 'Lost',
                                    detail_fetch_reason = 'listing_unavailable',
                                    detail_fetched_at = CURRENT_TIMESTAMP,
                                    needs_detail_refresh = 0,
                                    last_updated = CURRENT_TIMESTAMP,
                                    last_updated_source = 'AUTO'
                                WHERE id = ?
                                  AND (status IS NULL OR status != 'Lost')
                            """,
                            (row_id,),
                        )
                        conn.commit()
                        context.close()
                    continue

                soup = BeautifulSoup(page.content(), "html.parser")

                mv_id = extract_mv_id(soup)
                if not mv_id:
                    print("⚠️ MV ID missing — quarantined")
                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET needs_detail_refresh = 0,
                                detail_fetch_reason = 'mv_id_not_found',
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

                if find_existing_mv_owner(conn, mv_id, row_id):
                    print(f"⚠️ Duplicate MV {mv_id} — quarantined")
                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET needs_detail_refresh = 0,
                                detail_fetch_reason = 'duplicate_mv_id',
                                last_updated = CURRENT_TIMESTAMP,
                                last_updated_source = 'AUTO'
                            WHERE id = ?
                            """,
                            (row_id,),
                        )
                        conn.commit()
                        context.close()
                        continue

                # ---------------- PDF (clean + scoped) ----------------
                pdf_path = PDF_ROOT / f"{mv_id}.pdf"

                page.add_style_tag(content="""
                body * { visibility: hidden !important; }
                #hero, #hero *,
                div.teaser-content, div.teaser-content * {
                    visibility: visible !important;
                }
                header, footer, nav, button,
                .cookie-banner, #onetrust-consent-sdk,
                .cta, .back-link, aside, iframe {
                    display: none !important;
                }
                """)

                page.wait_for_timeout(500)
                page.emulate_media(media="print")
                if not DRY_RUN:
                    page.pdf(
                        path=str(pdf_path),
                        format="A4",
                        margin={
                            "top": "15mm",
                            "bottom": "15mm",
                            "left": "15mm",
                            "right": "15mm",
                        },
                        print_background=True,
                    )

                    if not pdf_path.exists() or pdf_path.stat().st_size < 10_000:
                        raise RuntimeError("PDF generation failed")

                # ---------------- Extract ----------------
                title = text_or_none(soup.select_one("#hero h1"))
                location = text_or_none(soup.select_one("#hero p.location"))
                hero_desc = text_or_none(soup.select_one("#hero p"))
                description = extract_teaser_field(soup, "Business Description") or hero_desc

                financials = extract_b4s_financials(soup)

                # ---------- FLATTEN → DB SCHEMA (SINGLE SOURCE OF TRUTH) ----------
                revenue_k = financials.get("turnover", {}).get("value_k")
                ebitda_k = financials.get("ebitda", {}).get("value_k")
                profit_margin_pct = financials.get("profit_margin", {}).get("pct")
                revenue_growth_pct = financials.get("revenue_growth", {}).get("pct")
                leverage_pct = financials.get("leverage", {}).get("pct")

                content_hash = compute_content_hash(title, location, description)

                # ---------- DRY RUN GUARD ----------
                if DRY_RUN:
                    print("🔍 DRY RUN – mapped DB values")
                    print("  revenue_k:", revenue_k)
                    print("  ebitda_k:", ebitda_k)
                    print("  profit_margin_pct:", profit_margin_pct)
                    print("  revenue_growth_pct:", revenue_growth_pct)
                    print("  leverage_pct:", leverage_pct)

                    for name, val in {
                        "revenue_k": revenue_k,
                        "ebitda_k": ebitda_k,
                        "profit_margin_pct": profit_margin_pct,
                        "revenue_growth_pct": revenue_growth_pct,
                        "leverage_pct": leverage_pct,
                    }.items():
                        assert val is None or isinstance(val, (int, float)), \
                            f"{name} not numeric: {val} ({type(val)})"

                    context.close()
                    continue

                if not DRY_RUN:
                    # ---------------- Drive ----------------
                    parent_folder_id = get_drive_parent_folder_id(
                        industry=BASE_INDUSTRY,
                        broker="BusinessesForSale",
                    )

                    deal_folder_id = find_or_create_deal_folder(
                        parent_folder_id=parent_folder_id,
                        deal_id=f"BFS-{mv_id}",
                        deal_title=title,
                    )
                    pdf_hash = compute_file_hash(pdf_path)

                    pdf_drive_url = upload_pdf_to_drive(
                        local_path=pdf_path,
                        filename=f"{mv_id}.pdf",
                        folder_id=deal_folder_id,
                    )
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM deal_artifacts
                        WHERE deal_id = ?
                          AND artifact_hash = ?
                          AND artifact_type = 'pdf'
                        """,
                        (row_id, pdf_hash),
                    ).fetchone()
                    deal_identity = f"B4S-{mv_id}"

                    if not existing:
                        record_deal_artifact(
                            conn=conn,
                            source="BusinessesForSale",
                            source_listing_id=mv_id,
                            deal_id=row_id,
                            artifact_type="pdf",
                            artifact_name=f"{mv_id}.pdf",
                            artifact_hash=pdf_hash,
                            drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                            drive_url=pdf_drive_url,
                            extraction_version=B4S_EXTRACTION_VERSION,
                            created_by="enrich_businesses4sale.py",
                        )

                    pdf_path.unlink(missing_ok=True)

                    # ---------------- DB UPDATE ----------------
                    conn.execute(
                        """
                        UPDATE deals
                        SET
                            source_listing_id = CASE
                                WHEN source_listing_id IS NULL THEN ?
                                ELSE source_listing_id
                            END,
                            title = ?,
                            location = ?,
                            description = ?,
                            content_hash = ?,
    
                            revenue_k = ?,
                            ebitda_k = ?,
                            profit_margin_pct = ?,
                            revenue_growth_pct = ?,
                            leverage_pct = ?,
    
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
                            mv_id,
                            title,
                            location,
                            description,
                            content_hash,

                            revenue_k,
                            ebitda_k,
                            profit_margin_pct,
                            revenue_growth_pct,
                            leverage_pct,

                            BASE_INDUSTRY,
                            BASE_SECTOR,
                            BASE_CONFIDENCE,
                            BASE_REASON,

                            pdf_drive_url,
                            deal_folder_id,
                            deal_folder_id,
                            datetime.utcnow().isoformat(timespec="seconds"),
                            row_id,
                        ),
                    )
                    conn.commit()

                print(f"✅ Enriched BFS-{mv_id}")
                context.close()
                time.sleep(random.uniform(*SLEEP_BETWEEN))

        finally:
            browser.close()
            conn.close()

    print("\n🏁 BusinessesForSale enrichment complete")

if __name__ == "__main__":
    enrich_businesses4sale(limit=None)