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

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/bb_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------
# EXTRACTION HELPERS
# -------------------------------------------------

def _extract_ref_id(soup: BeautifulSoup) -> Optional[str]:
    """
    Extracts BusinessBuyers REF ID, e.g. 'REF: 51106'
    """
    ref_el = soup.find("p", string=re.compile(r"REF:\s*\d+", re.I))
    if not ref_el:
        return None

    m = re.search(r"REF:\s*(\d+)", ref_el.get_text())
    return m.group(1) if m else None


def _extract_raw_sector(soup: BeautifulSoup) -> Optional[str]:
    """
    Extracts broker-declared sector from BusinessBuyers listing page.
    This is authoritative and must be preferred over keyword inference.
    """

    h5 = soup.select_one(".sub-sector-lease h5")
    if not h5:
        return None

    sector = h5.get_text(strip=True)
    return sector or None


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


def _extract_price(soup: BeautifulSoup) -> tuple[Optional[str], Optional[float]]:
    price_el = soup.select_one(".price-ref p")
    if not price_el:
        return None, None

    raw = price_el.get_text(strip=True)
    m = re.search(r"£\s*([\d,]+)", raw)
    if not m:
        return None, None

    price = f"£{m.group(1)}"
    price_k = float(m.group(1).replace(",", "")) / 1_000
    return price, price_k


# -------------------------------------------------
# ENRICHMENT
# -------------------------------------------------

def enrich_businessbuyers(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
            source_url,
            title
        FROM deals
        WHERE source = 'BusinessBuyers'
          AND (
                needs_detail_refresh = 1
                OR description IS NULL
                OR detail_fetched_at IS NULL
          )
        ORDER BY last_seen DESC
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    print(f"🔍 Found {len(rows)} BusinessBuyers deals needing enrichment")

    if not rows:
        conn.close()
        print("✅ Nothing to enrich")
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

            # -------------------------------------------------
            # FETCH + PDF (CLIENT GUARANTEE)
            # -------------------------------------------------
            # First fetch to discover REF ID
            html = bb.fetch_detail_anon_with_pdf(
                url,
                PDF_ROOT / "scratch.pdf",  # temporary, will be replaced
            )
            soup = BeautifulSoup(html, "html.parser")

            ref_id = _extract_ref_id(soup)
            if not ref_id:
                raise RuntimeError("BusinessBuyers REF ID not found")

            deal_identity = f"BB-{ref_id}"
            pdf_path = PDF_ROOT / f"{deal_identity}.pdf"

            # Final fetch with correct PDF name
            html = bb.fetch_detail_anon_with_pdf(url, pdf_path)
            soup = BeautifulSoup(html, "html.parser")

            if not pdf_path.exists() or pdf_path.stat().st_size < 10_000:
                raise RuntimeError(f"PDF not created or empty: {pdf_path}")

            # -------------------------------------------------
            # EXTRACT
            # -------------------------------------------------
            title = _extract_title(soup) or r["title"]
            description = _extract_description(soup)
            asking_price, asking_price_k = _extract_price(soup)

            raw_sector = _extract_raw_sector(soup)
            mapping = map_businessbuyers_sector(raw_sector=raw_sector)

            fetched_at = datetime.utcnow().isoformat(timespec="seconds")

            # -------------------------------------------------
            # DRIVE
            # -------------------------------------------------
            parent_folder_id = get_drive_parent_folder_id(
                industry=mapping["industry"],
                broker="BusinessBuyers",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_folder_id,
                deal_id=deal_identity,
                deal_title=title,
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{ref_id}.pdf",
                folder_id=deal_folder_id,
            )

            drive_file_id = pdf_drive_url.split("/d/")[1].split("/")[0]

            record_deal_artifact(
                conn=conn,
                deal_id=row_id,
                broker="BusinessBuyers",
                artifact_type="pdf",
                artifact_name=f"{ref_id}.pdf",
                drive_file_id=drive_file_id,
                drive_url=pdf_drive_url,
                industry=mapping["industry"],
                sector=mapping["sector"],
                created_by="enrich_businessbuyers.py",
            )

            pdf_path.unlink(missing_ok=True)

            # -------------------------------------------------
            # DB UPDATE (ATOMIC)
            # -------------------------------------------------
            cur = conn.execute(
                """
                UPDATE deals
                SET
                    source_listing_id           = ?,

                    title                       = ?,
                    description                 = ?,
                    asking_price                = ?,
                    asking_price_k              = ?,

                    industry                    = ?,
                    sector                      = ?,
                    sector_source               = 'broker',
                    sector_inference_confidence = ?,
                    sector_inference_reason     = ?,

                    drive_folder_id             = ?,
                    drive_folder_url            =
                        'https://drive.google.com/drive/folders/' || ?,
                    pdf_drive_url               = ?,
                    pdf_path                    = NULL,

                    detail_fetched_at           = ?,
                    needs_detail_refresh        = 0,
                    detail_fetch_reason         = NULL,
                    last_updated                = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    ref_id,

                    title,
                    description,
                    asking_price,
                    asking_price_k,

                    mapping["industry"],
                    mapping["sector"],
                    mapping["confidence"],
                    mapping["reason"],

                    deal_folder_id,
                    deal_folder_id,
                    pdf_drive_url,
                    fetched_at,
                    row_id,
                ),
            )

            if cur.rowcount != 1:
                raise RuntimeError(f"Expected 1 row updated, got {cur.rowcount}")

            conn.commit()
            print(f"✅ Enriched + uploaded ({deal_identity})")

    finally:
        conn.close()

    print("\n🏁 BusinessBuyers enrichment complete")


if __name__ == "__main__":
    enrich_businessbuyers()