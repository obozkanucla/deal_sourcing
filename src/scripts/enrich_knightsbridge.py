# src/scripts/enrich_knightsbridge.py

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
from src.persistence.deal_artifacts import record_deal_artifact


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
        self.browser = self.playwright.chromium.launch(headless=os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1")
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

import re

import re

import re

MAX_TITLE_LEN = 80  # this is sane for Drive

def clean_and_shorten_title(title: str | None) -> str:
    if not title:
        return "Untitled Deal"

    t = title.strip()

    # Remove [None], (None), bare None anywhere
    t = re.sub(r"[\[\(]?\bnone\b[\]\)]?", "", t, flags=re.IGNORECASE)

    # Remove Knightsbridge boilerplate phrases
    boilerplate = [
        r"opportunity to acquire",
        r"well[- ]known",
        r"well[- ]established",
        r"established company",
        r"specialising in",
        r"alongside",
        r"serving",
        r"offering",
    ]
    for bp in boilerplate:
        t = re.sub(bp, "", t, flags=re.IGNORECASE)

    # Remove location suffixes like "- Surrey", "| London", etc.
    t = re.sub(r"\s*[-|–]\s*[A-Z][a-zA-Z\s]+$", "", t)

    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # HARD truncate (word-safe)
    if len(t) > MAX_TITLE_LEN:
        t = t[:MAX_TITLE_LEN]
        t = t.rsplit(" ", 1)[0]

    return t or "Untitled Deal"

def enrich_knightsbridge(limit: Optional[int] = None):
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
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
        row_id     = r["id"]  # ← primary key
        deal_id    = r["deal_id"]
        listing_id = r["source_listing_id"]
        pdf_path   = Path(r["pdf_path"])
        raw_title = r["title"]
        title = clean_and_shorten_title(raw_title)
        print(f"\n📁 Drive-only Knightsbridge {listing_id}")

        try:
            mapping = KNIGHTSBRIDGE_SECTOR_MAP[r["sector_raw"]]
            industry = mapping["industry"]
            sector   = mapping["sector"]

            parent_folder_id = get_drive_parent_folder_id(
                industry=industry,
                broker="Knightsbridge",
            )

            listing_id = r["source_listing_id"]
            if not listing_id:
                raise RuntimeError("source_listing_id is required for Knightsbridge")

            canonical_id = f"KB-{listing_id}"

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

            record_deal_artifact(
                conn=conn,
                deal_id=row_id,
                broker="Knightsbridge",  # or Knightsbridge
                artifact_type="pdf",
                artifact_name=f"{listing_id}.pdf",
                drive_file_id=drive_file_id,
                drive_url=pdf_drive_url,
                industry=mapping["industry"],
                sector=mapping["sector"],
                created_by="enrich_knightsbridge.py",
            )

            pdf_path.unlink(missing_ok=True)
            drive_folder_url = f"https://drive.google.com/drive/folders/{deal_folder_id}"

            cur = conn.execute(
                """
                UPDATE deals
                SET
                    industry = ?,
                    sector = ?,
                    sector_source = 'broker',
                    sector_inference_confidence = ?,
                    sector_inference_reason = ?,
                    drive_folder_url = ?,
                    drive_folder_id = ?,
                    pdf_drive_url = ?,
                    pdf_path = NULL,
                    needs_detail_refresh = 0,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    industry,
                    sector,
                    mapping["confidence"],
                    mapping["reason"],
                    deal_folder_id,
                    drive_folder_url,
                    pdf_drive_url,
                    row_id,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
            conn.commit()
            print("✅ Uploaded + cleaned (Drive-only)")

        except Exception as e:
            cur = conn.execute(
                """
                UPDATE deals
                SET needs_detail_refresh = 1,
                    detail_fetch_reason = ?
                WHERE id = ?
                """,
                (str(e)[:500], row_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
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
            row_id = r["id"]
            deal_id    = r["deal_id"]
            listing_id = r["source_listing_id"]
            url        = r["source_url"]
            sector_raw = r["sector_raw"]
            raw_title = r["title"]
            title = clean_and_shorten_title(raw_title)

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

                cur = conn.execute(
                    "UPDATE deals SET pdf_path = ? WHERE id = ?",
                    (str(pdf_path), row_id),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
                conn.commit()

                mapping = KNIGHTSBRIDGE_SECTOR_MAP[sector_raw]
                industry = mapping["industry"]
                sector   = mapping["sector"]

                parent_folder_id = get_drive_parent_folder_id(
                    industry=industry,
                    broker="Knightsbridge",
                )

                listing_id = r["source_listing_id"]
                if not listing_id:
                    raise RuntimeError("source_listing_id is required for Knightsbridge")

                canonical_id = f"KB-{listing_id}"

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

                pdf_path.unlink(missing_ok=True)

                fetched_at = datetime.utcnow().isoformat(timespec="seconds")
                drive_folder_url = f"https://drive.google.com/drive/folders/{deal_folder_id}"

                cur = conn.execute(
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
                        drive_folder_url = ?,
                        pdf_drive_url = ?,
                        pdf_path = NULL,
                        detail_fetched_at = ?,
                        needs_detail_refresh = 0,
                        detail_fetch_reason = NULL,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        description,
                        price,
                        industry,
                        sector,
                        mapping["confidence"],
                        mapping["reason"],
                        deal_folder_id,
                        drive_folder_url,
                        pdf_drive_url,
                        fetched_at,
                        row_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
                conn.commit()

                print("✅ Enriched + uploaded")

            except Exception as e:
                cur = conn.execute(
                    """
                    UPDATE deals
                    SET needs_detail_refresh = 1,
                        detail_fetch_reason = ?
                    WHERE id = ?
                    """,
                    (str(e)[:500], row_id),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
                conn.commit()
                print(f"❌ Error: {e}")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Knightsbridge enrichment complete")


if __name__ == "__main__":
    enrich_knightsbridge()