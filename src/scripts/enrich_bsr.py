"""
Business Sale Report (BSR) enrichment

Contract:
- Enrichment only (no import)
- Capture raw broker-declared data only
- PDF + artifact creation
- Deduplicate via canonical_external_id
- DRY_RUN supported
"""
import os
import csv
import re
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
from src.utils.financial_normalization import _normalize_money_to_k
from src.sector_mappings.bsr import BSR_SECTOR_MAP

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SOURCE = "BusinessSaleReport"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/bsr_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DETAIL_WAIT_SELECTOR = "body"
SLEEP_BETWEEN = (2, 4)

DRY_RUN = False
HEADLESS = True

BSR_EXTRACTION_VERSION = "v1-detail"

CAPTURE_CSV = Path("/tmp/bsr_sector_raw_capture.csv")

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def extract_bsr_sector_raw(soup: BeautifulSoup) -> Optional[str]:
    for row in soup.select("table.basic-table tr"):
        label = row.select_one("td b")
        value = row.select_one("td + td")
        if not label or not value:
            continue
        if label.get_text(strip=True).lower() == "sector:":
            return value.get_text(strip=True)
    return None


def extract_bsr_financials(soup: BeautifulSoup) -> dict:
    out = {"revenue_k": None, "asking_price_k": None}
    for p in soup.select("p.bsr-blue"):
        text = p.get_text(" ", strip=True)
        if "turnover" in text.lower():
            out["revenue_k"] = _normalize_money_to_k(text)
        elif "asking price" in text.lower():
            out["asking_price_k"] = _normalize_money_to_k(text)
    return out


def extract_web_reference(soup: BeautifulSoup) -> Optional[str]:
    for b in soup.find_all("b"):
        if "web reference" in b.get_text(strip=True).lower():
            parent = b.parent
            if parent:
                m = re.search(r"Web Reference:\s*(\d+)", parent.get_text(" ", strip=True))
                if m:
                    return m.group(1)
    return None


def extract_kv_table(soup: BeautifulSoup) -> dict[str, str]:
    data = {}
    for row in soup.select("table.basic-table tr"):
        cells = row.select("td")
        if len(cells) == 2:
            key = cells[0].get_text(strip=True).rstrip(":")
            val = cells[1].get_text(strip=True)
            data[key] = val
    return data


def extract_location(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.select_one("h1")
    if not h1:
        return None
    row = h1.find_next("div", class_="row")
    if not row:
        return None
    p = row.select_one("p")
    return p.get_text(strip=True) if p else None


def is_bsr_sold_listing(soup: BeautifulSoup) -> bool:
    h1 = soup.select_one("h1")
    if not h1:
        return False  # true parser failure, not SOLD

    h1_text = h1.get_text(strip=True).lower()

    terminal_phrases = [
        "no longer in our listings",
        "no business found",
    ]

    return any(p in h1_text for p in terminal_phrases)


# -------------------------------------------------
# MAIN
# -------------------------------------------------
MAX_RUNTIME_SECONDS = int(
    os.getenv("MAX_RUNTIME_SECONDS", 75 * 60)
)

def enrich_bsr(
    limit: Optional[int] = None,
    min_id: Optional[int] = None,
) -> None:
    job_started_at = time.time()

    print(f"📀 SQLite DB path: {DB_PATH}")
    print(f"🏷️ BSR enrichment starting | DRY_RUN={DRY_RUN}")

    repo = SQLiteRepository(DB_PATH)
    conn = repo.get_conn()

    deals = repo.fetch_deals_for_enrichment(source=SOURCE)
    if min_id is not None:
        deals = [d for d in deals if d["id"] >= min_id]
    if limit:
        deals = deals[:limit]

    print(f"🔍 {len(deals)} BSR deals to enrich")
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
                if time.time() - job_started_at > MAX_RUNTIME_SECONDS:
                    print("⏱️ Max runtime reached, exiting cleanly")
                    break
                url = deal["source_url"]

                print(f"\n➡️ [{i}/{len(deals)}]")
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

                if is_bsr_sold_listing(soup):
                    print("🏁 SOLD / removed listing detected")

                    if not DRY_RUN:
                        conn.execute(
                            """
                            UPDATE deals
                            SET status               = 'Lost',
                                needs_detail_refresh = 0,
                                last_updated         = CURRENT_TIMESTAMP,
                                last_updated_source  = 'AUTO'
                            WHERE id = ?
                            """,
                            (deal["id"],),
                        )
                        conn.commit()

                    context.close()
                    continue

                title_el = soup.select_one("h1")
                title = title_el.get_text(strip=True) if title_el else None

                canonical_external_id = extract_web_reference(soup)
                sector_raw = extract_bsr_sector_raw(soup)

                # --- Canonical sector resolution (mandatory) ---
                if sector_raw:
                    mapping = BSR_SECTOR_MAP.get(sector_raw.lower())
                    if mapping:
                        industry = mapping["industry"]
                        sector = mapping["sector"]
                        sector_confidence = mapping["confidence"]
                        sector_reason = mapping["reason"]
                    else:
                        industry = "Other"
                        sector = "Other"
                        sector_confidence = 0.4
                        sector_reason = f"BSR unmapped sector_raw: {sector_raw}"
                else:
                    industry = "Other"
                    sector = "Other"
                    sector_confidence = 0.4
                    sector_reason = "BSR listing without declared sector (explicit fallback)"

                if not title or not canonical_external_id:
                    print("⚠️ Missing critical fields")
                    context.close()
                    continue

                if DRY_RUN:
                    if canonical_external_id in captured_ids:
                        print("⏭ already captured, skipping")
                        context.close()
                        continue

                    print("🔍 DRY RUN – capture only")
                    print({
                        "canonical_external_id": canonical_external_id,
                        "sector_raw": sector_raw,
                    })

                    csv_writer.writerow({
                        "source_listing_id": canonical_external_id,
                        "sector_raw": sector_raw,
                    })
                    csv_file.flush()
                    captured_ids.add(canonical_external_id)

                    context.close()
                    continue

                # ---------------- FULL ENRICHMENT (DRY_RUN=False) ----------------

                kv = extract_kv_table(soup)
                financials = extract_bsr_financials(soup)
                location_raw = extract_location(soup)

                content_hash = compute_content_hash(
                    title=title,
                    description=title,  # gated content
                    location=location_raw or "",
                )

                pdf_path = PDF_ROOT / f"{canonical_external_id}.pdf"

                page.add_style_tag(content="""
                header, footer, nav, button, iframe,
                .cookie-banner, .cta {
                    display: none !important;
                }
                """)

                page.wait_for_timeout(400)
                page.emulate_media(media="print")

                page.pdf(
                    path=str(pdf_path),
                    format="A4",
                    print_background=True,
                )

                pdf_hash = compute_file_hash(pdf_path)

                parent_folder_id = get_drive_parent_folder_id(
                    industry=industry,
                    broker="BusinessSaleReport",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=f"BSR-{canonical_external_id}",
                    deal_title=title,
                )

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{canonical_external_id}.pdf",
                    folder_id=deal_folder_id,
                )

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        title = ?,
                        location = ?,
                        sector_raw = ?,
                        
                        industry = ?,
                        sector = ?,
                        sector_source = 'bsr',
                        sector_inference_confidence = ?,
                        sector_inference_reason = ?,
                        
                        canonical_external_id = ?,
                        revenue_k = ?,
                        asking_price_k = ?,
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
                        title,
                        location_raw,
                        sector_raw,
                        industry,
                        sector,
                        sector_confidence,
                        sector_reason,
                        canonical_external_id,
                        financials["revenue_k"],
                        financials["asking_price_k"],
                        content_hash,
                        deal_folder_id,
                        deal_folder_id,
                        datetime.utcnow().isoformat(),
                        deal["id"],
                    ),
                )
                conn.commit()

                record_deal_artifact(
                    conn=conn,
                    source=SOURCE,
                    source_listing_id=canonical_external_id,
                    deal_id=deal["id"],
                    artifact_type="pdf",
                    artifact_name=f"{canonical_external_id}.pdf",
                    artifact_hash=pdf_hash,
                    drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                    drive_url=pdf_drive_url,
                    extraction_version=BSR_EXTRACTION_VERSION,
                    created_by="enrich_bsr.py",
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

    print("🏁 BSR enrichment complete")


if __name__ == "__main__":
    enrich_bsr()