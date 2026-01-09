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
from src.sector_mappings.dealopportunities import map_dealopportunities_sector


# =========================================================
# CONFIG
# =========================================================

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DRY_RUN = False # os.getenv("DRY_RUN", "0") == "1"

# =========================================================
# HELPERS
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
        val = dd.get_text(strip=True) if dd else None
        if not val:
            continue

        if "sector" in label:
            facts["sector"] = val
        elif "region" in label:
            facts["region"] = val
        elif "reference" in label:
            facts["reference"] = val

    return {
        "description": description,
        "facts": facts,
    }


def extract_do_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1 > a[href*='/opportunity/']")
    return h1.get_text(strip=True) if h1 else None


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
            sector_raw,
            turnover_range_raw,
            revenue_k
        FROM deals
        WHERE source = 'DealOpportunities'
          AND detail_fetched_at IS NULL
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
        total = len(rows)

        for idx, r in enumerate(rows, start=1):
            completed = idx + 1
            row_id = r["id"]
            deal_id = r["source_listing_id"]
            url = r["source_url"]

            print(f"\n[{idx}/{total}] ➜ {deal_id}")
            print(f"➡️ Fetching detail page:\n   {url}")

            pdf_path = PDF_ROOT / f"{deal_id}.pdf"

            # HARD recycle every 25 deals
            if completed % 25 == 0:
                print("🔄 Restarting DealOpportunities browser")
                client.stop()
                client.start()
                # client._human_sleep()

            try:
                html = client.fetch_listing_detail_and_pdf(
                    url=url,
                    pdf_path=pdf_path,
                )
            except PlaywrightError as e:
                print("  ❌ Detail fetch failed")
                if not DRY_RUN:
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

            # ✅ CRITICAL: cooldown after each deal
            client._cooldown()


            title = extract_do_title(html) or r["title"]
            parsed = parse_do_detail(html)

            # -------------------------------------------------
            # Revenue inference (from IMPORTED turnover)
            # -------------------------------------------------
            if r["revenue_k"] is None:
                inferred = map_turnover_range_to_revenue_k(
                    r["turnover_range_raw"]
                )
                if inferred is not None and not DRY_RUN:
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

            # -------------------------------------------------
            # Sector / industry
            # -------------------------------------------------
            raw_sector = r["sector_raw"]
            mapping = map_dealopportunities_sector(raw_sector=raw_sector)

            # -------------------------------------------------
            # Drive folder + PDF upload
            # -------------------------------------------------
            drive_folder_url = None
            drive_folder_id = None
            pdf_drive_url = None
            pdf_error = None

            try:
                parent_id = get_drive_parent_folder_id(
                    industry=mapping["industry"],
                    broker="DealOpportunities",
                )

                drive_folder_id = find_or_create_deal_folder(
                    parent_folder_id=parent_id,
                    deal_id=deal_id,
                    deal_title=title,
                )

                drive_folder_url = (
                    f"https://drive.google.com/drive/folders/{drive_folder_id}"
                )

                pdf_drive_url = upload_pdf_to_drive(
                    local_path=pdf_path,
                    filename=f"{deal_id}.pdf",
                    folder_id=drive_folder_id,
                )

            except (HttpError, RuntimeError) as e:
                pdf_error = str(e)[:500]
                print("  ⚠️ PDF / Drive failed")

            # -------------------------------------------------
            # Final DB update
            # -------------------------------------------------
            if not DRY_RUN:
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
                        pdf_generated_at = ?,
                        pdf_error = ?,

                        detail_fetched_at = ?,
                        last_updated = ?,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                    """,
                    (
                        title,
                        parsed["description"],
                        json.dumps(parsed["facts"]),
                        hashlib.sha256(html.encode()).hexdigest(),

                        mapping["industry"],
                        mapping["sector"],
                        mapping["confidence"],
                        mapping["reason"],

                        drive_folder_id,
                        drive_folder_url,
                        pdf_drive_url,
                        datetime.utcnow().isoformat(timespec="seconds"),
                        pdf_error,

                        datetime.utcnow().isoformat(timespec="seconds"),
                        datetime.utcnow().isoformat(timespec="seconds"),
                        row_id,
                    ),
                )
                conn.commit()

            pdf_path.unlink(missing_ok=True)
            print("  ✅ Enriched")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 DealOpportunities enrichment complete")


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    limit_env = None #os.getenv("ENRICH_LIMIT")
    limit = None if not limit_env else int(limit_env)
    enrich_dealopportunities(limit)