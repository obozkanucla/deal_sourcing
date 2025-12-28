import sqlite3
from pathlib import Path
from datetime import datetime
import re
import json
from typing import Optional

from src.brokers.axispartnership_client import AxisPartnershipClient


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

    # ------------------------------------------------------------------
    # Find deals needing enrichment (additive + idempotent)
    # ------------------------------------------------------------------
    query = """
        SELECT
            id,
            source_listing_id,
            source_url
        FROM deals
        WHERE source = 'AxisPartnership'
          AND (
                needs_detail_refresh = 1
                OR description IS NULL
                OR detail_fetched_at IS NULL
          )
        ORDER BY source_listing_id
    """

    rows = conn.execute(query).fetchall()
    if limit:
        rows = rows[:limit]

    print(f"🔍 Found {len(rows)} Axis deals needing details")

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    client = AxisPartnershipClient()
    client.start()

    try:
        for row in rows:
            deal_id = row["id"]
            listing_id = row["source_listing_id"]
            url = row["source_url"]

            print(f"\n➡️ Enriching Axis {listing_id}")
            print(url)

            pdf_path = PDF_ROOT / f"{listing_id}.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)

            # ----------------------------------------------------------
            # Fetch detail + PDF
            # ----------------------------------------------------------
            html = client.fetch_detail_and_pdf(url, pdf_path)

            description = _extract_description(client.page)
            kpis = _extract_kpis(html)

            fetched_at = datetime.utcnow().isoformat(timespec="seconds")

            # ----------------------------------------------------------
            # ATOMIC UPDATE (ALL FIELDS WIRED)
            # ----------------------------------------------------------
            conn.execute(
                """
                UPDATE deals
                SET
                    description = ?,
                    extracted_json = ?,
                    pdf_path = ?,
                    detail_fetched_at = ?,
                    needs_detail_refresh = 0,
                    detail_fetch_reason = NULL,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    description,
                    json.dumps(kpis) if kpis else None,
                    str(pdf_path),
                    fetched_at,
                    deal_id,
                ),
            )

            conn.commit()

            print(f"✅ Enriched Axis {listing_id}")
            print(f"   Description: {'YES' if description else 'NO'}")
            print(f"   PDF: {pdf_path}")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Axis detail enrichment complete")


if __name__ == "__main__":
    enrich_axispartnership()