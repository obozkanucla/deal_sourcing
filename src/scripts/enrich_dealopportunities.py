import sqlite3
from pathlib import Path
from datetime import datetime
import hashlib
import json
import os
import time
from typing import Optional

from bs4 import BeautifulSoup
from playwright._impl._errors import Error as PlaywrightError
from googleapiclient.errors import HttpError

from src.brokers.dealopportunities_client import DealOpportunitiesClient
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.persistence.deal_artifacts import record_deal_artifact
from src.sector_mappings.dealopportunities import map_dealopportunities_sector


# =========================================================
# CONFIG
# =========================================================

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DRY_RUN = os.getenv("DRY_RUN", "0") == "1"


# =========================================================
# PARSING (DETAIL PAGE ONLY)
# =========================================================

def parse_do_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    desc_el = soup.select_one(".opportunity-description, .content, article")
    description = desc_el.get_text("\n", strip=True) if desc_el else None

    if not description:
        table_td = soup.select_one("table td[valign='top']")
        if table_td:
            description = table_td.get_text("\n", strip=True)

    facts = {}
    for dt in soup.select("dl dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        value = dd.get_text(strip=True) if dd else None
        if not value:
            continue

        if "sector" in label:
            facts["sector"] = value
        elif "region" in label:
            facts["region"] = value
        elif "reference" in label:
            facts["reference"] = value

    return {
        "description": description,
        "facts": facts,
    }


def extract_do_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    h1_link = soup.select_one("h1 > a[href*='/opportunity/']")
    return h1_link.get_text(strip=True) if h1_link else None


# =========================================================
# TURNOVER → REVENUE (IMPORT-ONLY INPUT)
# =========================================================

def map_turnover_range_to_revenue_k(raw: str | None) -> float | None:
    if not raw:
        return None

    s = raw.lower().strip()

    ranges = {
        "under £500k": 250,
        "£500k–£1m": 750,
        "£1m–£2m": 1500,
        "£2m–£5m": 3500,
        "£5m–£10m": 7500,
        "£10m+": 15000,
    }

    for k, v in ranges.items():
        if k.replace("–", "-") in s or k in s:
            return float(v)

    return None


# =========================================================
# ENRICHMENT
# =========================================================

def enrich_dealopportunities(limit: Optional[int] = None) -> None:
    print("=" * 72)
    print("🧠 Enriching DealOpportunities")
    print(f"📀 SQLite DB: {DB_PATH}")
    print(f"🧪 DRY_RUN={DRY_RUN}")
    print("=" * 72)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            source_url,
            title,
            turnover_range_raw,
            revenue_k
        FROM deals
        WHERE source = 'DealOpportunities'
          AND (
            pdf_generated_at IS NULL
            OR pdf_error IS NOT NULL
          )
        ORDER BY last_seen DESC
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    client = DealOpportunitiesClient()
    client.start()

    try:
        for idx, r in enumerate(rows, start=1):
            row_id = r["id"]
            deal_key = r["source_listing_id"]
            url = r["source_url"]

            print(f"\n[{idx}/{len(rows)}] ➜ {deal_key}")

            if DRY_RUN:
                print("  🧪 DRY RUN — skipping fetch")
                continue

            conn.execute(
                """
                UPDATE deals
                SET pdf_error = NULL,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (row_id,),
            )
            conn.commit()

            pdf_path = PDF_ROOT / f"{deal_key}.pdf"

            try:
                html = client.fetch_listing_detail_and_pdf(url, pdf_path)
            except PlaywrightError as e:
                conn.execute(
                    """
                    UPDATE deals
                    SET pdf_error = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (str(e)[:500], row_id),
                )
                conn.commit()
                continue

            title = extract_do_title(html) or r["title"]
            parsed = parse_do_detail(html)

            # -----------------------------------------
            # Revenue inference (ONLY if missing)
            # -----------------------------------------
            if r["revenue_k"] is None:
                inferred = map_turnover_range_to_revenue_k(
                    r["turnover_range_raw"]
                )
                if inferred is not None:
                    conn.execute(
                        """
                        UPDATE deals
                        SET revenue_k = ?,
                            last_updated = CURRENT_TIMESTAMP,
                            last_updated_source = 'AUTO'
                        WHERE id = ?
                        """,
                        (inferred, row_id),
                    )

            # -----------------------------------------
            # Sector / Industry
            # -----------------------------------------
            mapping = map_dealopportunities_sector(
                raw_sector=parsed["facts"].get("sector")
            )

            content_hash = hashlib.sha256(html.encode()).hexdigest()

            # -----------------------------------------
            # Drive + PDF
            # -----------------------------------------
            try:
                parent_folder_id = get_drive_parent_folder_id(
                    industry=mapping["industry"],
                    broker="DealOpportunities",
                )

                deal_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_folder_id,
                    deal_id=deal_key,
                    deal_title=title,
                )

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{deal_key}.pdf",
                    folder_id=deal_folder_id,
                )

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        title = ?,
                        description = ?,
                        extracted_json = ?,
                        content_hash = ?,
                        industry = ?,
                        sector = ?,
                        sector_source = 'broker',
                        sector_inference_confidence = ?,
                        sector_inference_reason = ?,
                        drive_folder_id = ?,
                        drive_folder_url = ?,
                        pdf_drive_url = ?,
                        pdf_generated_at = CURRENT_TIMESTAMP,
                        pdf_error = NULL,
                        last_updated = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                    """,
                    (
                        title,
                        parsed["description"],
                        json.dumps(parsed["facts"]),
                        content_hash,
                        mapping["industry"],
                        mapping["sector"],
                        mapping["confidence"],
                        mapping["reason"],
                        deal_folder_id,
                        f"https://drive.google.com/drive/folders/{deal_folder_id}",
                        pdf_drive_url,
                        row_id,
                    ),
                )

                record_deal_artifact(
                    conn=conn,
                    deal_id=row_id,
                    broker="DealOpportunities",
                    artifact_type="pdf",
                    artifact_name=f"{deal_key}.pdf",
                    drive_file_id=pdf_drive_url.split("/d/")[1].split("/")[0],
                    drive_url=pdf_drive_url,
                    industry=mapping["industry"],
                    sector=mapping["sector"],
                    created_by="enrich_dealopportunities.py",
                )

                conn.commit()
                print("  ✅ Enriched")

            except (HttpError, Exception) as e:
                conn.execute(
                    """
                    UPDATE deals
                    SET pdf_error = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (str(e)[:500], row_id),
                )
                conn.commit()
                print("  ⚠️ PDF / Drive failed")

            finally:
                pdf_path.unlink(missing_ok=True)

    finally:
        client.stop()
        conn.close()

    print("\n🏁 DealOpportunities enrichment complete")


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    limit_env = os.getenv("ENRICH_LIMIT")
    limit = None if not limit_env else int(limit_env)
    enrich_dealopportunities(limit)