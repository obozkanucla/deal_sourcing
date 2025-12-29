from src.config import KB_USERNAME, KB_PASSWORD

import os
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
from src.sector_mappings.knightsbridge import KNIGHTSBRIDGE_SECTOR_MAP


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/knightsbridge_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

KNIGHTSBRIDGE_BASE = "https://www.knightsbridgeplc.com"

RESTART_EVERY = 50
BASE_SLEEP = 1.2
JITTER = 0.8

if not KB_USERNAME or not KB_PASSWORD:
    raise RuntimeError("KB_USERNAME / KB_PASSWORD not set")


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def _sleep(extra: float = 0.0):
    time.sleep(BASE_SLEEP + extra + (JITTER * os.urandom(1)[0] / 255))


def _extract_description(page) -> Optional[str]:
    try:
        blocks = page.locator("#BusinessDetails p")
        texts = []
        for i in range(blocks.count()):
            t = blocks.nth(i).inner_text().strip()
            if len(t) > 40:
                texts.append(t)
        return "\n\n".join(texts) if texts else None
    except Exception:
        return None


def _extract_price(page) -> Optional[str]:
    try:
        price = page.locator(".btn.price")
        if price.count():
            return price.first.inner_text().strip()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------
# CLIENT (UNCHANGED, WORKING LOGIN)
# ---------------------------------------------------------------------

class KnightsbridgeClient:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None

    def start(self):
        print("🚀 Starting Knightsbridge client")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()

    def stop(self):
        print("🛑 Stopping Knightsbridge client")
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def login(self):
        print("🔐 Logging into Knightsbridge")
        self.page.goto(f"{KNIGHTSBRIDGE_BASE}/login/", wait_until="domcontentloaded")
        self.page.fill("#LoginEmail", KB_USERNAME)
        self.page.fill("#LoginPassword", KB_PASSWORD)
        self.page.evaluate("LoginUser('#ContentPlaceHolder1_ctl09')")
        self.page.wait_for_timeout(3000)

        if self.page.locator("text=Logout").count() == 0:
            raise RuntimeError("Knightsbridge login failed")

        print("✅ Logged in successfully")


# ---------------------------------------------------------------------
# ENRICHMENT
# ---------------------------------------------------------------------
def goto_with_retry(page, url, retries=2):
    for attempt in range(retries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            return
        except PlaywrightTimeout:
            if attempt == retries - 1:
                raise
            time.sleep(2)

def enrich_knightsbridge(limit: Optional[int] = None):
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            deal_id,
            title,
            sector_raw,
            thesis_ready,
            source_listing_id,
            source_url,
            description,
            asking_price,
            pdf_path,
            pdf_drive_url
        FROM deals
        WHERE source = 'Knightsbridge'
          AND (
                needs_detail_refresh = 1
                OR detail_fetched_at IS NULL
                OR description IS NULL
                OR (pdf_path IS NOT NULL AND pdf_drive_url IS NULL)
              )
        ORDER BY source_listing_id
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    if not rows:
        print("✅ Nothing to enrich")
        return

    drive_only = []
    needs_browser = []

    for r in rows:
        pdf_path = Path(r["pdf_path"]) if r["pdf_path"] else None
        if (
            r["description"]
            and r["asking_price"]
            and pdf_path
            and pdf_path.exists()
            and not r["pdf_drive_url"]
        ):
            drive_only.append(r)
        else:
            needs_browser.append(r)

    print(
        f"🧠 Plan: {len(needs_browser)} browser enrich | "
        f"{len(drive_only)} Drive-only"
    )

    # ------------------------------------------------------------------
    # DRIVE-ONLY (NO BROWSER)
    # ------------------------------------------------------------------

    for r in drive_only:
        deal_id    = r["deal_id"]
        listing_id = r["source_listing_id"]
        pdf_path   = Path(r["pdf_path"])

        print(f"\n📁 Drive-only Knightsbridge {listing_id}")

        try:
            mapping = KNIGHTSBRIDGE_SECTOR_MAP[r["sector_raw"]]
            industry = mapping["industry"]
            sector   = mapping["sector"]

            parent_folder_id = get_drive_parent_folder_id(
                industry=industry,
                broker="Knightsbridge",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_folder_id,
                deal_id=deal_id,
                deal_title=r["title"],
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=str(pdf_path),
                filename=f"{listing_id}.pdf",
                folder_id=deal_folder_id,
            )

            pdf_path.unlink(missing_ok=True)

            conn.execute(
                """
                UPDATE deals
                SET
                    industry = ?,
                    sector = ?,
                    sector_source = 'broker',
                    sector_inference_confidence = ?,
                    sector_inference_reason = ?,
                    drive_folder_id = ?,
                    pdf_drive_url = ?,
                    pdf_path = NULL,
                    needs_detail_refresh = 0,
                    last_updated = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (
                    industry,
                    sector,
                    mapping["confidence"],
                    mapping["reason"],
                    deal_folder_id,
                    pdf_drive_url,
                    deal_id,
                ),
            )
            conn.commit()
            print("✅ Uploaded + cleaned (Drive-only)")

        except Exception as e:
            conn.execute(
                """
                UPDATE deals
                SET needs_detail_refresh = 1,
                    detail_fetch_reason = ?
                WHERE deal_id = ?
                """,
                (str(e)[:500], deal_id),
            )
            conn.commit()
            print(f"❌ Drive-only error: {e}")

    # ------------------------------------------------------------------
    # BROWSER REQUIRED
    # ------------------------------------------------------------------

    if not needs_browser:
        conn.close()
        print("\n🏁 Knightsbridge enrichment complete (Drive-only)")
        return

    client = KnightsbridgeClient()
    client.start()
    client.login()

    processed = 0

    try:
        for r in needs_browser:
            deal_id    = r["deal_id"]
            listing_id = r["source_listing_id"]
            url        = r["source_url"]
            sector_raw = r["sector_raw"]

            processed += 1
            if processed % RESTART_EVERY == 0:
                client.stop()
                client = KnightsbridgeClient()
                client.start()
                client.login()

            print(f"\n➡️ Enriching Knightsbridge {listing_id}")
            full_url = url if url.startswith("http") else f"{KNIGHTSBRIDGE_BASE}{url}"

            try:
                goto_with_retry(client.page,full_url)
                client.page.wait_for_selector("#BusinessDetails", timeout=15_000)
                _sleep(0.6)

                description = _extract_description(client.page)
                price = _extract_price(client.page)

                pdf_path = PDF_ROOT / f"{listing_id}.pdf"
                client.page.pdf(path=str(pdf_path), format="A4", print_background=True)

                conn.execute(
                    "UPDATE deals SET pdf_path = ? WHERE deal_id = ?",
                    (str(pdf_path), deal_id),
                )
                conn.commit()

                mapping = KNIGHTSBRIDGE_SECTOR_MAP[sector_raw]
                industry = mapping["industry"]
                sector   = mapping["sector"]

                parent_folder_id = get_drive_parent_folder_id(
                    industry=industry,
                    broker="Knightsbridge",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=deal_id,
                    deal_title=r["title"],
                )

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=str(pdf_path),
                    filename=f"{listing_id}.pdf",
                    folder_id=deal_folder_id,
                )

                pdf_path.unlink(missing_ok=True)

                fetched_at = datetime.utcnow().isoformat(timespec="seconds")

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        description = ?,
                        asking_price = ?,
                        industry = ?,
                        sector = ?,
                        sector_source = 'broker',
                        sector_inference_confidence = ?,
                        sector_inference_reason = ?,
                        drive_folder_id = ?,
                        pdf_drive_url = ?,
                        pdf_path = NULL,
                        detail_fetched_at = ?,
                        needs_detail_refresh = 0,
                        detail_fetch_reason = NULL,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE deal_id = ?
                    """,
                    (
                        description,
                        price,
                        industry,
                        sector,
                        mapping["confidence"],
                        mapping["reason"],
                        deal_folder_id,
                        pdf_drive_url,
                        fetched_at,
                        deal_id,
                    ),
                )
                conn.commit()

                print("✅ Enriched + uploaded")

            except Exception as e:
                conn.execute(
                    """
                    UPDATE deals
                    SET needs_detail_refresh = 1,
                        detail_fetch_reason = ?
                    WHERE deal_id = ?
                    """,
                    (str(e)[:500], deal_id),
                )
                conn.commit()
                print(f"❌ Error: {e}")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Knightsbridge enrichment complete")


if __name__ == "__main__":
    enrich_knightsbridge()