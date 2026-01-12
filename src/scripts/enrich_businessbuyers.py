# src/scripts/enrich_businessbuyers.py
import sqlite3
from pathlib import Path
from datetime import datetime
import re
from typing import Optional

from bs4 import BeautifulSoup

from src.brokers.businessbuyers_client import BusinessBuyersClient
from src.config import BB_USERNAME, BB_PASSWORD
from src.persistence.deal_artifacts import record_deal_artifact

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

from src.sector_mappings.businessbuyers import map_businessbuyers_sector
from src.utils.hash_utils import compute_file_hash
from src.persistence.repository import SQLiteRepository
# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/bb_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)
BB_EXTRACTION_VERSION = "v1"

# -------------------------------------------------
# LOST DETECTION
# -------------------------------------------------

LOST_PHRASES = [
    "oops, 404 error",
    "we couldn't find the page",
    "page not found",
    "no longer available",
    "business has been sold",
    "opportunity withdrawn",
]
repo = SQLiteRepository()

def is_businessbuyers_lost(html: Optional[str]) -> bool:
    if not html:
        return False
    text = html.lower()
    return any(p in text for p in LOST_PHRASES)

# -------------------------------------------------
# EXTRACTION HELPERS
# -------------------------------------------------

def _extract_ref_id(soup: BeautifulSoup) -> Optional[str]:
    ref_el = soup.find("p", string=re.compile(r"REF:\s*\d+", re.I))
    if not ref_el:
        return None
    m = re.search(r"REF:\s*(\d+)", ref_el.get_text())
    return m.group(1) if m else None

def _extract_raw_sector(soup: BeautifulSoup) -> Optional[str]:
    h5 = soup.select_one(".sub-sector-lease h5")
    return h5.get_text(strip=True) if h5 else None

def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else None

def _extract_description(soup: BeautifulSoup) -> Optional[str]:
    points = []
    for div in soup.select("#overview .selling-point"):
        txt = div.get_text(" ", strip=True)
        if txt:
            points.append(txt)
    if not points:
        return None
    seen = set()
    clean = []
    for p in points:
        if p not in seen:
            clean.append(p)
            seen.add(p)
    return "\n".join(clean)

def _extract_price_k(soup: BeautifulSoup) -> Optional[int]:
    el = soup.select_one(".price-ref p")
    if not el:
        return None

    txt = el.get_text(strip=True)
    m = re.search(r"£\s*([\d,]+)", txt)
    if not m:
        return None

    return int(m.group(1).replace(",", "")) // 1_000

# -------------------------------------------------
# DATABASE HELPER
# -------------------------------------------------

def _find_existing_ref_owner(conn, ref_id: str, current_row_id: int) -> Optional[int]:
    row = conn.execute(
        """
        SELECT id
        FROM deals
        WHERE source = 'BusinessBuyers'
          AND source_listing_id = ?
          AND id != ?
        """,
        (ref_id, current_row_id),
    ).fetchone()
    return row["id"] if row else None

# -------------------------------------------------
# ENRICHMENT
# -------------------------------------------------

def enrich_businessbuyers(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = repo.fetch_deals_for_enrichment(
        source="BusinessBuyers",
    )

    if limit:
        rows = rows[:limit]

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    bb = BusinessBuyersClient(
        username=BB_USERNAME,
        password=BB_PASSWORD,
        click_budget=None,
    )
    bb.login()

    try:
        for r in rows:
            row_id = r["id"]
            url = r["source_url"]

            print("\n➡️ Enriching BusinessBuyers deal")
            print(url)

            try:
                html = bb.fetch_detail_anon_with_pdf(
                    url,
                    PDF_ROOT / "scratch.pdf",
                )
            except Exception as e:
                print(f"⚠️ Fetch error — retry later: {e}")
                conn.execute(
                    "UPDATE deals SET detail_fetch_reason='fetch_error' WHERE id=?",
                    (row_id,),
                )
                conn.commit()
                continue

            if is_businessbuyers_lost(html):
                print("❌ Deal marked Lost (404 / removed)")
                # Only fire Slack if this is a NEW transition
                cur = conn.execute(
                    """
                    UPDATE deals
                    SET status               = 'Lost',
                        needs_detail_refresh = 0,
                        detail_fetch_reason  = 'listing_removed',
                        last_updated         = CURRENT_TIMESTAMP
                    WHERE id = ?
                      AND (status IS NULL OR status != 'Lost')
                    """,
                    (row_id,),
                )

                conn.commit()

                if cur.rowcount != 1:
                    from src.integrations.slack import SlackNotifier

                    SlackNotifier().send_message(
                        title="Deal marked Lost",
                        text=(
                            f"*Source:* BusinessBuyers\n"
                            f"*URL:* {url}\n"
                            f"*Reason:* listing removed (404)"
                        ),
                        level="warning",
                    )

                continue

            soup = BeautifulSoup(html, "html.parser")

            ref_id = _extract_ref_id(soup)
            if not ref_id:
                print("❌ REF missing — marking Lost")
                conn.execute(
                    """
                    UPDATE deals
                    SET status='Lost',
                        needs_detail_refresh=0,
                        detail_fetch_reason='ref_missing_assumed_lost',
                        last_updated=CURRENT_TIMESTAMP,
                        last_updated_source='AUTO'
                    WHERE id=?
                    """,
                    (row_id,),
                )
                conn.commit()
                continue

            existing_owner_id = _find_existing_ref_owner(conn, ref_id, row_id)
            if existing_owner_id:
                conn.execute(
                    """
                    UPDATE deals
                    SET needs_detail_refresh=0,
                        detail_fetch_reason='duplicate_businessbuyers_ref'
                    WHERE id=?
                    """,
                    (row_id,),
                )
                conn.commit()
                continue

            deal_identity = f"BB-{ref_id}"
            pdf_path = PDF_ROOT / f"{deal_identity}.pdf"

            try:
                html = bb.fetch_detail_anon_with_pdf(url, pdf_path)
            except Exception:
                conn.execute(
                    "UPDATE deals SET detail_fetch_reason='fetch_error_after_ref' WHERE id=?",
                    (row_id,),
                )
                conn.commit()
                continue

            if is_businessbuyers_lost(html):
                conn.execute(
                    """
                    UPDATE deals
                    SET status='Lost',
                        needs_detail_refresh=0,
                        detail_fetch_reason='broker_confirmed_lost',
                        last_updated=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (row_id,),
                )
                conn.commit()
                continue

            if not pdf_path.exists() or pdf_path.stat().st_size < 10_000:
                conn.execute(
                    "UPDATE deals SET detail_fetch_reason='pdf_failed' WHERE id=?",
                    (row_id,),
                )
                conn.commit()
                continue

            soup = BeautifulSoup(html, "html.parser")

            title = _extract_title(soup) or r["title"]
            description = _extract_description(soup)
            asking_price_k = _extract_price_k(soup)

            raw_sector = _extract_raw_sector(soup)
            mapping = map_businessbuyers_sector(raw_sector=raw_sector)
            sector_source = "broker" if mapping["confidence"] >= 0.9 else "unclassified"

            parent_folder_id = get_drive_parent_folder_id(
                industry=mapping["industry"],
                broker="BusinessBuyers",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_folder_id,
                deal_id=deal_identity,
                deal_title=title,
                # month_prefix="2512"
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{ref_id}.pdf",
                folder_id=deal_folder_id,
            )
            pdf_hash = compute_file_hash(pdf_path)

            record_deal_artifact(
                conn=conn,
                source="BusinessBuyers",
                source_listing_id=ref_id,
                deal_id=row_id,  # optional, fine to pass
                artifact_type="pdf",
                artifact_name=f"{ref_id}.pdf",
                artifact_hash=pdf_hash,
                drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                drive_url=pdf_drive_url,
                extraction_version=BB_EXTRACTION_VERSION,
                created_by="enrich_businessbuyers.py",
            )

            pdf_path.unlink(missing_ok=True)

            conn.execute(
                """
                UPDATE deals
                SET source_listing_id           = ?,
                    title                       = ?,
                    description                 = ?,

                    asking_price_k = CASE
                        WHEN asking_price_k IS NULL THEN ?
                        ELSE asking_price_k
                    END,
                    revenue_k                   = NULL,
                    ebitda_k                    = NULL,

                    industry                    = ?,
                    sector                      = ?,
                    sector_source               = ?,
                    sector_inference_confidence = ?,
                    sector_inference_reason     = ?,

                    drive_folder_id             = ?,
                    drive_folder_url            = 'https://drive.google.com/drive/folders/' || ?,
                    pdf_drive_url               = ?,

                    detail_fetched_at           = ?,
                    needs_detail_refresh        = 0,
                    last_updated                = CURRENT_TIMESTAMP,
                    last_updated_source         = 'AUTO'
                WHERE id = ?
                """,
                (
                    ref_id,
                    title,
                    description,
                    asking_price_k,

                    mapping["industry"],
                    mapping["sector"],
                    sector_source,
                    mapping["confidence"],
                    mapping["reason"],

                    deal_folder_id,
                    deal_folder_id,
                    pdf_drive_url,

                    datetime.utcnow().isoformat(timespec="seconds"),
                    row_id,
                ),
            )
            conn.commit()
            print(f"✅ Enriched + uploaded ({deal_identity})")

    finally:
        conn.close()

    print("\n🏁 BusinessBuyers enrichment complete")


if __name__ == "__main__":
    enrich_businessbuyers()