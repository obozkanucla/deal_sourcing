import sqlite3
from pathlib import Path
from datetime import datetime
import hashlib
import json
import os
from typing import Optional

from bs4 import BeautifulSoup

from src.brokers.dealopportunities_client import DealOpportunitiesClient
from src.persistence.deal_artifacts import record_deal_artifact

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)

from src.sector_mappings.dealopportunities import map_dealopportunities_sector


# -------------------------------------------------
# CONFIG
# -------------------------------------------------

PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"


# -------------------------------------------------
# PARSER
# -------------------------------------------------

def parse_do_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # --- description ---
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
        elif "turnover" in label:
            facts["turnover"] = value
        elif "reference" in label:
            facts["reference"] = value

    return {
        "description": description,
        "facts": facts,
    }

def extract_do_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # DealOpportunities: title is always <h1><a href="/opportunity/...">...</a></h1>
    h1_link = soup.select_one("h1 > a[href*='/opportunity/']")
    if not h1_link:
        return None

    title = h1_link.get_text(strip=True)
    return title if title else None

# -------------------------------------------------
# ENRICHMENT
# -------------------------------------------------
def normalize_do_sector(raw_sector: str | None) -> str | None:
    if not raw_sector:
        return None

    # Split comma-separated sectors
    parts = [p.strip() for p in raw_sector.split(",") if p.strip()]
    if not parts:
        return None

    # Canonical normalization
    sector = parts[0]
    sector = sector.replace("\u00a0", " ")   # NBSP → space
    sector = sector.strip()
    sector = sector.title()                  # normalize casing

    return sector

def enrich_dealopportunities(limit: Optional[int] = None) -> None:
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 🔁 Crash recovery (ONE-TIME, SAFE)
    conn.execute(
        """
        UPDATE deals
        SET
            detail_fetch_reason = 'retry_after_crash',
            last_updated        = CURRENT_TIMESTAMP
        WHERE source = 'DealOpportunities'
          AND detail_fetch_reason = 'in_progress'
          AND detail_fetched_at IS NULL
        """
    )
    conn.commit()

    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            source_url,
            title
        FROM deals
        WHERE source = 'DealOpportunities'
          AND (
            detail_fetched_at IS NULL
          )
        ORDER BY last_seen DESC
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    print(f"🔍 Found {len(rows)} DealOpportunities deals needing enrichment")

    total = len(rows)

    if not rows:
        conn.close()
        print("✅ Nothing to enrich")
        return

    client = DealOpportunitiesClient()
    client.start()

    try:
        for idx, r in enumerate(rows):
            row_id = r["id"]
            deal_key = r["source_listing_id"]
            url = r["source_url"]
            completed = idx + 1
            pct = completed * 100 // total

            print(
                f"\n📊 Progress: {completed}/{total} ({pct}%)"
            )
            conn.execute(
                """
                UPDATE deals
                SET needs_detail_refresh = 0,
                    detail_fetch_reason  = 'in_progress',
                    last_updated         = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (row_id,)
            )
            conn.commit()

            print(f"\n➡️ Enriching {deal_key}")
            print(url)

            pdf_path = PDF_ROOT / f"{deal_key}.pdf"

            # -------------------------------------------------
            # FETCH + PDF
            # -------------------------------------------------
            html = client.fetch_listing_detail_and_pdf(
                url=url,
                pdf_path=pdf_path,
            )
            client._cooldown()
            title = extract_do_title(html) or r["title"]
            parsed = parse_do_detail(html)

            # -------------------------------------------------
            # MAPPING (broker sector is authoritative)
            # -------------------------------------------------
            raw_sector = normalize_do_sector(parsed["facts"].get("sector"))
            mapping = map_dealopportunities_sector(raw_sector=raw_sector)

            # -------------------------------------------------
            # HASH / TIMESTAMP
            # -------------------------------------------------
            content_hash = hashlib.sha256(html.encode()).hexdigest()
            fetched_at = datetime.utcnow().isoformat(timespec="seconds")

            # -------------------------------------------------
            # DRIVE
            # -------------------------------------------------
            parent_folder_id = get_drive_parent_folder_id(
                industry=mapping["industry"],
                broker="DealOpportunities",
            )

            deal_folder_id = find_or_create_deal_folder(
                parent_folder_id=parent_folder_id,
                deal_id=deal_key,
                deal_title=title,
            )

            drive_folder_url = (
                f"https://drive.google.com/drive/folders/{deal_folder_id}"
            )

            pdf_drive_url = upload_pdf_to_drive(
                local_path=pdf_path,
                filename=f"{deal_key}.pdf",
                folder_id=deal_folder_id,
            )

            drive_file_id = pdf_drive_url.split("/d/")[1].split("/")[0]

            # -------------------------------------------------
            # ARTIFACT RECORD (STANDARD)
            # -------------------------------------------------
            record_deal_artifact(
                conn=conn,
                deal_id=row_id,
                broker="DealOpportunities",
                artifact_type="pdf",
                artifact_name=f"{deal_key}.pdf",
                drive_file_id=drive_file_id,
                drive_url=pdf_drive_url,
                industry=mapping["industry"],
                sector=mapping["sector"],
                created_by="enrich_dealopportunities.py",
            )

            # -------------------------------------------------
            # DEAL UPDATE
            # -------------------------------------------------
            cur = conn.execute(
                """
                UPDATE deals
                SET
                    title                       = ?,
                    description                 = ?,
                    extracted_json              = ?,
                    content_hash                = ?,

                    industry                    = ?,
                    sector                      = ?,
                    sector_source               = 'broker',
                    sector_inference_confidence = ?,
                    sector_inference_reason     = ?,
                    
                    drive_folder_id             = ?,
                    drive_folder_url            = ?,

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
                    parsed["description"],
                    json.dumps(parsed["facts"]),
                    content_hash,

                    mapping["industry"],
                    mapping["sector"],
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
                raise RuntimeError(f"Expected 1 row updated, got {cur.rowcount}")

            conn.commit()

            pdf_path.unlink(missing_ok=True)
            print(f"✅ Enriched + uploaded {deal_key}")

    finally:
        client.stop()
        conn.close()

    print("\n🏁 DealOpportunities enrichment complete")


# -------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------

if __name__ == "__main__":
    limit_env = os.getenv("ENRICH_LIMIT")
    limit = None if limit_env in (None, "", "none") else int(limit_env)

    enrich_dealopportunities(limit=limit)