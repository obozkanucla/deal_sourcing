# src/scripts/enrich_knightsbridge.py

from src.config import KB_USERNAME, KB_PASSWORD

import os
import re
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.sector_mappings.knightsbridge import KNIGHTSBRIDGE_SECTOR_MAP
from src.persistence.deal_artifacts import record_deal_artifact
from src.utils.hash_utils import compute_file_hash
from src.brokers.knightsbridge_client import KnightsbridgeClient
from src.persistence.repository import SQLiteRepository

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
repo = SQLiteRepository(Path("db/deals.sqlite"))
KNIGHTSBRIDGE_EXTRACTION_VERSION = "v1"
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/knightsbridge_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

KNIGHTSBRIDGE_BASE = "https://www.knightsbridgeplc.com"

RESTART_EVERY = 50
BASE_SLEEP = 1.2
JITTER = 0.8

if not KB_USERNAME or not KB_PASSWORD:
    raise RuntimeError("KB_USERNAME / KB_PASSWORD not set")

DRY_RUN = False

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def is_knightsbridge_lost_page(page) -> bool:
    text = page.content().lower()
    return (
        "business no longer exists" in text
        or "this business is no longer available" in text
        or "listing not found" in text
    )


def has_lost_text(page) -> bool:
    text = page.content().lower()
    indicators = [
        "no longer available",
        "listing not found",
        "business withdrawn",
        "sold subject to contract",
        "this opportunity has been removed",
    ]
    return any(i in text for i in indicators)

def assert_knightsbridge_not_lost(page, response):
    if response is None:
        raise RuntimeError("LISTING_LOST_NAV")

    if response.status in (404, 410):
        raise RuntimeError("LISTING_LOST_HTTP")

    if is_knightsbridge_lost_page(page):
        raise RuntimeError("LISTING_LOST_DOM")

    if "login" in page.url.lower():
        raise RuntimeError("LISTING_LOST_LOGIN_REDIRECT")

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


def _extract_asking_price_k(page) -> Optional[int]:
    """
    Extract asking price from Knightsbridge UI and return £k.
    """
    try:
        price = page.locator(".btn.price")
        if not price.count():
            return None

        raw = price.first.inner_text().strip()
        # e.g. "£4,000,000"
        m = re.search(r"£\s*([\d,]+)", raw)
        if not m:
            return None

        pounds = int(m.group(1).replace(",", ""))
        return pounds // 1_000  # £ → £k

    except Exception:
        return None

def normalize_knightsbridge_sector(s: str | None) -> str | None:
    if not s:
        return None
    return (
        s.replace("\xa0", " ")
         .replace("&amp;", "&")
         .strip()
    )
# ---------------------------------------------------------------------
# ENRICHMENT
# ---------------------------------------------------------------------
def goto_with_retry(page, url, retries=2):
    for attempt in range(retries):
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            return response
        except PlaywrightTimeout:
            if attempt == retries - 1:
                raise RuntimeError("LISTING_LOST_TIMEOUT")
            time.sleep(2)

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

    rows = repo.fetch_deals_for_enrichment(
        source="Knightsbridge",
    )

    if limit:
        rows = rows[:limit]

    if not rows:
        print("✅ Nothing to enrich")
        return

    client = KnightsbridgeClient()
    client.start()
    client.login()

    processed = 0

    try:
        for r in rows:
            error: Exception | None = None
            success = False
            row_id = r["id"]
            listing_id = r["source_listing_id"]
            url        = r["source_url"]
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
                response = goto_with_retry(client.page,full_url)

                try:
                    client.page.wait_for_selector("#BusinessDetails", timeout=15_000)
                except PlaywrightTimeout:
                    print("PlaywrightTimeout waiting for #BusinessDetails")
                    # Only now can we evaluate LOST
                    client.page.reload(wait_until="domcontentloaded")
                    try:
                        client.page.wait_for_selector("#BusinessDetails", timeout=10_000)
                    except PlaywrightTimeout:
                        raise RuntimeError("LISTING_LOST_DOM_TIMEOUT")

                description = _extract_description(client.page)
                asking_price_k = _extract_asking_price_k(client.page)
                if asking_price_k is None:
                    print(f"⚠️ Knightsbridge {listing_id}: asking price not visible")

                pdf_path = PDF_ROOT / f"{listing_id}.pdf"
                client.page.pdf(path=str(pdf_path), format="A4", print_background=True)
                industry = r["industry"]
                sector = r["sector"]
                sector_confidence = 1.0
                sector_reason = "broker"

                if not industry or not sector:
                    raise RuntimeError("MISSING_SECTOR_CANONICAL")

                # Knightsbridge sector is broker-declared at index time.
                # Enrichment must never infer or override it.
                assert industry and sector

                if not industry or not sector:
                    raise RuntimeError("MISSING_SECTOR_CANONICAL")

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
                        source="Knightsbridge",
                        source_listing_id=str(listing_id),
                        deal_id=row_id,
                        artifact_type="pdf",
                        artifact_name=f"{listing_id}.pdf",
                        artifact_hash=pdf_hash,
                        drive_file_id=drive_file_id,
                        drive_url=pdf_drive_url,
                        extraction_version=KNIGHTSBRIDGE_EXTRACTION_VERSION,
                        created_by="enrich_knightsbridge.py",
                    )
                fetched_at = datetime.utcnow().isoformat(timespec="seconds")
                drive_folder_url = f"https://drive.google.com/drive/folders/{deal_folder_id}"
                if DRY_RUN:
                    print("DRY_RUN → would UPDATE deals:", row_id)
                    print("Price:", asking_price_k)

                    print("URL:", client.page.url)
                    print("HTTP status:", response.status if response else None)
                    print("Title:", client.page.title())
                    print("Body snippet:", client.page.content()[:500])
                    client.page.screenshot(path=f"/tmp/kb_{listing_id}.png")
                    success = True
                else:
                    cur = conn.execute(
                        """
                        UPDATE deals
                         SET
                            description                 = ?,
                            asking_price_k              = COALESCE(asking_price_k, ?),
                        
                            industry                    = ?,
                            sector                      = ?,
                            sector_source               = 'broker',
                            sector_inference_confidence = ?,
                            sector_inference_reason     = ?,
                        
                            drive_folder_id             = ?,
                            drive_folder_url            = ?,
                            pdf_drive_url               = ?,
                        
                            detail_fetched_at           = ?,
                            needs_detail_refresh        = 0,
                            detail_fetch_reason         = NULL,
                            last_updated                = CURRENT_TIMESTAMP,
                            last_updated_source         = 'AUTO'
                        WHERE id = ?
                        """,
                        (
                            description,
                            asking_price_k,
                            industry,
                            sector,
                            sector_confidence,
                            sector_reason,
                            deal_folder_id,
                            drive_folder_url,
                            pdf_drive_url,
                            fetched_at,
                            row_id,
                        )
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
                    conn.commit()

                    print("✅ Enriched + uploaded")
                    success = True
            except Exception as exc:
                error = exc
                reason = str(exc) if exc else "UNKNOWN_EXCEPTION"
                print("EXCEPTION STR:", reason)
                if success:
                    # This was a clean DRY_RUN or successful write
                    continue
                if reason == "MISSING_SECTOR_CANONICAL":
                    if DRY_RUN:
                        print("DRY_RUN → would park deal due to missing canonical sector:", row_id)
                    else:
                        conn.execute(
                            """
                            UPDATE deals
                            SET detail_fetched_at    = CURRENT_TIMESTAMP,
                                needs_detail_refresh = 0,
                                detail_fetch_reason  = 'MISSING_SECTOR_CANONICAL',
                                last_updated         = CURRENT_TIMESTAMP,
                                last_updated_source  = 'AUTO'
                            WHERE id = ?
                            """,
                            (row_id,),
                        )
                        conn.commit()
                    continue
                if "LISTING_LOST" in reason:
                    lost_reason = reason
                    if DRY_RUN:
                        print("DRY_RUN → would UPDATE deals:", row_id)
                    else:
                        conn.execute(
                            """
                            UPDATE deals
                            SET status               = 'Lost',
                                lost_reason          = ?,
                                needs_detail_refresh = 0,
                                last_updated         = CURRENT_TIMESTAMP,
                                detail_fetched_at    = CURRENT_TIMESTAMP,
                                last_updated_source  = 'AUTO'
                            WHERE id = ?
                            """,
                            (lost_reason, row_id,),
                        )
                        conn.commit()
                        print(f"🗑️ Marked Knightsbridge {listing_id} as LOST")
                        success = True
                    continue
                if DRY_RUN:
                    print("DRY_RUN → would UPDATE deals:", row_id)
                else:
                    cur = conn.execute(
                        """
                        UPDATE deals
                        SET needs_detail_refresh = 1,
                            detail_fetch_reason = ?,
                            last_updated_source = 'AUTO'
                        WHERE id = ?
                        """,
                        (reason[:500], row_id),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(f"Expected 1 row, got {cur.rowcount}")
                    conn.commit()
                    success = True
                print(f"❌ Error: {reason}")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Knightsbridge enrichment complete")


if __name__ == "__main__":
    enrich_knightsbridge()