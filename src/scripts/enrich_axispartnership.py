import sqlite3
from pathlib import Path
from datetime import datetime
import re
import json
from typing import Optional

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.brokers.axispartnership_client import AxisPartnershipClient
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.sector_mappings.axis import infer_axis_industry_sector
from src.persistence.deal_artifacts import record_deal_artifact

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/axis_pdfs")


# ------------------------------------------------------------------
# EXTRACTION HELPERS
# ------------------------------------------------------------------

def _extract_description(page) -> Optional[str]:
    """
    Extracts the main deal description from Axis detail pages.
    This intentionally ignores index excerpts.
    """
    try:
        candidates = [
            "article .entry-content",
            "div.cz_post_content",
            "div.cz_content",
            "div.post-content",
        ]

        for selector in candidates:
            block = page.locator(selector).first
            if block.count() == 0:
                continue

            text = block.inner_text().strip()
            if text and len(text) > 200:
                return text

        return None

    except Exception:
        return None

def _extract_title(page) -> Optional[str]:
    try:
        h1 = page.locator("h1.xtra-post-title-headline").first
        if h1.count():
            return h1.inner_text().strip()
    except Exception:
        pass
    return None

def clean_axis_title(title: str, listing_id: str | int) -> str:
    if not title:
        return title

    title = title.strip()

    # Normalize dash variants just in case
    normalized = title.replace("–", "-").replace("—", "-")

    suffix = f"- {listing_id}"
    if normalized.endswith(suffix):
        return normalized[: -len(suffix)].strip()

    return title


def _extract_kpis(html: str) -> dict:
    """
    Lightweight, intentionally conservative extraction.
    Raw strings only — normalization happens later.
    """
    out: dict[str, str] = {}

    m_turnover = re.search(r"(Turnover[^£]*£[\d,\.]+)", html, re.I)
    if m_turnover:
        out["turnover"] = m_turnover.group(1)

    m_ebitda = re.search(r"(EBITDA[^£]*£[\d,\.]+)", html, re.I)
    if m_ebitda:
        out["ebitda"] = m_ebitda.group(1)

    if re.search(r"UNDER OFFER", html, re.I):
        out["status"] = "under_offer"
    else:
        out["status"] = "for_sale"

    return out


# ------------------------------------------------------------------
# ENRICHMENT
# ------------------------------------------------------------------

def enrich_axispartnership(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            source_url,
            title
        FROM deals
        WHERE source = 'AxisPartnership'
          AND (
                needs_detail_refresh = 1
                OR description IS NULL
                OR detail_fetched_at IS NULL
          )
        ORDER BY source_listing_id
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    print(f"🔍 Found {len(rows)} Axis deals needing details")

    if not rows:
        conn.close()
        print("✅ Nothing to enrich")
        return

    client = AxisPartnershipClient()
    client.start()

    try:
        for r in rows:
            row_id     = r["id"]
            listing_id = r["source_listing_id"]
            url        = r["source_url"]

            print(f"\n➡️ Enriching Axis {listing_id}")

            pdf_path = PDF_ROOT / f"{listing_id}.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)

            html = client.fetch_detail_and_pdf(url, pdf_path)
            page_title = _extract_title(client.page)
            clean_title = clean_axis_title(
                title=page_title,
                listing_id=listing_id,
            )

            title = clean_title or r["title"]

            description = _extract_description(client.page)
            mapping = infer_axis_industry_sector(
                title=title,
                description=description,
            )
            kpis        = _extract_kpis(html)
            fetched_at  = datetime.utcnow().isoformat(timespec="seconds")

            # ---- Drive resolution ----
            parent_folder_id = get_drive_parent_folder_id(
                industry=mapping["industry"],
                broker="AxisPartnership",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_folder_id,
                deal_id=str(listing_id),
                deal_title=title,
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
                broker="AxisPartnership",  # or Knightsbridge
                artifact_type="pdf",
                artifact_name=f"{listing_id}.pdf",
                drive_file_id=drive_file_id,
                drive_url=pdf_drive_url,
                industry=mapping["industry"],
                sector=mapping["sector"],
                created_by="enrich_axispartnership.py",
            )

            pdf_path.unlink(missing_ok=True)

            cur = conn.execute(
                """
                UPDATE deals
                SET title                       = ?,
                    description                 = ?,
                    extracted_json              = ?,

                    industry                    = ?,
                    sector                      = ?,
                    sector_source               = 'inferred',
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
                    title,
                    description,
                    json.dumps(kpis) if kpis else None,

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

            print("✅ Enriched + uploaded")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Axis detail enrichment complete")


if __name__ == "__main__":
    enrich_axispartnership()