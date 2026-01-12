# src/scripts/enrich_hiltonsmythe.py

import os
import re
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.persistence.deal_artifacts import record_deal_artifact
from src.utils.hash_utils import compute_file_hash
from src.persistence.repository import SQLiteRepository

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
repo = SQLiteRepository(Path("db/deals.sqlite"))
HS_EXTRACTION_VERSION = "v1"
BROKER = "HiltonSmythe"

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/hiltonsmythe_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

RESTART_EVERY = 40
BASE_SLEEP = 1.1
JITTER = 0.7

DRY_RUN = False

# ---------------------------------------------------------------------
# SLEEP
# ---------------------------------------------------------------------
def _sleep(extra: float = 0.0):
    time.sleep(BASE_SLEEP + extra + (JITTER * os.urandom(1)[0] / 255))

# ---------------------------------------------------------------------
# LOST DETECTION
# ---------------------------------------------------------------------
def is_hiltonsmythe_lost(page) -> bool:
    text = page.content().lower()
    indicators = [
        "no longer available",
        "this business is no longer available",
        "listing not found",
        "opportunity withdrawn",
        "sold subject to contract",
    ]
    return any(i in text for i in indicators)

def assert_not_lost(page, response):
    if response is None:
        raise RuntimeError("LISTING_LOST_NAV")

    if response.status in (404, 410):
        raise RuntimeError("LISTING_LOST_HTTP")

    if is_hiltonsmythe_lost(page):
        raise RuntimeError("LISTING_LOST_DOM")

# ---------------------------------------------------------------------
# EXTRACTION
# ---------------------------------------------------------------------
def extract_structured_description(page) -> Optional[str]:
    """
    Dump all meaningful content into a structured markdown-like block.
    Contact details are intentionally excluded.
    """
    root = page.locator("#theme-content-section")
    if not root.count():
        return None

    sections = []

    def grab_block(title: str, selector: str):
        blocks = page.locator(selector)
        if not blocks.count():
            return
        lines = []
        for i in range(blocks.count()):
            t = blocks.nth(i).inner_text().strip()
            if len(t) > 30:
                lines.append(t)
        if lines:
            sections.append(f"## {title}\n" + "\n".join(lines))

    grab_block("Overview", ".tcb-post-content p")
    grab_block("Financial Highlights", ".tcb-styled-list li")
    grab_block("Key Highlights", ".tcb-styled-list li")
    grab_block("Basis of Sale", ".tcb-styled-list li")

    return "\n\n".join(sections) if sections else None

# ---------------------------------------------------------------------
# NAVIGATION
# ---------------------------------------------------------------------
def goto_with_retry(page, url, retries=2):
    for attempt in range(retries):
        try:
            return page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PlaywrightTimeout:
            if attempt == retries - 1:
                raise RuntimeError("LISTING_LOST_TIMEOUT")
            time.sleep(2)

# ---------------------------------------------------------------------
# ENRICHMENT
# ---------------------------------------------------------------------
def enrich_hiltonsmythe(limit: Optional[int] = None):
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = repo.fetch_deals_for_enrichment(
        source="HiltonSmythe",
        freshness_days=14,
    )

    if limit:
        rows = rows[:limit]

    if not rows:
        print("✅ Nothing to enrich")
        return

    processed = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            for r in rows:
                processed += 1
                if processed % RESTART_EVERY == 0:
                    context.close()
                    context = browser.new_context()
                    page = context.new_page()

                row_id = r["id"]
                listing_id = r["source_listing_id"]
                url = r["source_url"]

                print(f"\n➡️ Enriching Hilton Smythe {listing_id}")

                try:
                    response = goto_with_retry(page, url)
                    assert_not_lost(page, response)

                    page.wait_for_selector("#theme-content-section", timeout=15_000)

                    description = extract_structured_description(page)

                    pdf_path = PDF_ROOT / f"{listing_id}.pdf"
                    page.pdf(path=str(pdf_path), format="A4", print_background=True)

                    industry = r["industry"]
                    sector = r["sector"]
                    if not industry or not sector:
                        raise RuntimeError("MISSING_SECTOR_CANONICAL")

                    parent_folder_id = get_drive_parent_folder_id(
                        industry=industry,
                        broker=BROKER,
                    )

                    canonical_id = f"HS-{listing_id}"

                    deal_folder_id = find_or_create_deal_folder(
                        parent_folder_id=parent_folder_id,
                        deal_id=canonical_id,
                        deal_title=r["title"],
                    )

                    pdf_drive_url = upload_pdf_to_drive(
                        local_path=str(pdf_path),
                        filename=f"{listing_id}.pdf",
                        folder_id=deal_folder_id,
                    )
                    drive_file_id = pdf_drive_url.split("/d/")[1].split("/")[0]

                    pdf_hash = compute_file_hash(pdf_path)
                    pdf_path.unlink(missing_ok=True)

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

                    if not existing:
                        record_deal_artifact(
                            conn=conn,
                            source=BROKER,
                            source_listing_id=str(listing_id),
                            deal_id=row_id,
                            artifact_type="pdf",
                            artifact_name=f"{listing_id}.pdf",
                            artifact_hash=pdf_hash,
                            drive_file_id=drive_file_id,
                            drive_url=pdf_drive_url,
                            extraction_version=HS_EXTRACTION_VERSION,
                            created_by="enrich_hiltonsmythe.py",
                        )

                    fetched_at = datetime.utcnow().isoformat(timespec="seconds")
                    drive_folder_url = (
                        f"https://drive.google.com/drive/folders/{deal_folder_id}"
                    )

                    if DRY_RUN:
                        print("DRY_RUN → would UPDATE deals:", row_id)
                        print(description[:500])
                    else:
                        conn.execute(
                            """
                            UPDATE deals
                            SET
                                description              = ?,
                                drive_folder_id          = ?,
                                drive_folder_url         = ?,
                                pdf_drive_url            = ?,
                                detail_fetched_at        = ?,
                                needs_detail_refresh     = 0,
                                detail_fetch_reason      = NULL,
                                last_updated             = CURRENT_TIMESTAMP,
                                last_updated_source      = 'AUTO'
                            WHERE id = ?
                            """,
                            (
                                description,
                                deal_folder_id,
                                drive_folder_url,
                                pdf_drive_url,
                                fetched_at,
                                row_id,
                            ),
                        )
                        conn.commit()
                        print("✅ Enriched + uploaded")

                except Exception as exc:
                    reason = str(exc)
                    print("❌ Error:", reason)

                    if "LISTING_LOST" in reason:
                        if DRY_RUN:
                            print("DRY_RUN → would mark LOST:", row_id)
                        else:
                            conn.execute(
                                """
                                UPDATE deals
                                SET status = 'Lost',
                                    lost_reason = ?,
                                    needs_detail_refresh = 0,
                                    detail_fetched_at = CURRENT_TIMESTAMP,
                                    last_updated = CURRENT_TIMESTAMP,
                                    last_updated_source = 'AUTO'
                                WHERE id = ?
                                """,
                                (reason, row_id),
                            )
                            conn.commit()
                            print(f"🗑️ Marked HS {listing_id} as LOST")
                    else:
                        if not DRY_RUN:
                            conn.execute(
                                """
                                UPDATE deals
                                SET needs_detail_refresh = 1,
                                    detail_fetch_reason = ?,
                                    last_updated_source = 'AUTO'
                                WHERE id = ?
                                """,
                                (reason[:500], row_id),
                            )
                            conn.commit()

                _sleep()

        finally:
            context.close()
            browser.close()
            conn.close()

    print("\n🏁 Hilton Smythe enrichment complete")


if __name__ == "__main__":
    enrich_hiltonsmythe()