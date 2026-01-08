import sqlite3
from pathlib import Path
from datetime import datetime
import hashlib
import json
import os
from typing import Optional

from bs4 import BeautifulSoup
from playwright._impl._errors import Error as PlaywrightError

from src.brokers.dealopportunities_client import DealOpportunitiesClient
from src.sector_mappings.dealopportunities import map_dealopportunities_sector


# =================================================
# CONFIG
# =================================================

PDF_ROOT = Path("/tmp/do_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

# -----------------
# DRY RUN CONTROLS
# -----------------
DRY_RUN = True
DRY_RUN_FETCH_HTML = True   # set False to skip Playwright entirely
DRY_RUN_LIMIT = 5           # hard safety cap


# =================================================
# PARSER (DETAIL ONLY — NO TURNOVER HERE)
# =================================================

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
    h1 = soup.select_one("h1 > a[href*='/opportunity/']")
    return h1.get_text(strip=True) if h1 else None


# =================================================
# TURNOVER → REVENUE (CANONICAL, SINGLE PLACE)
# =================================================

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


# =================================================
# ENRICHMENT
# =================================================

def enrich_dealopportunities(limit: Optional[int] = None) -> None:
    print("=" * 72)
    print("🧪 DRY RUN MODE — NO DATA WILL BE MODIFIED" if DRY_RUN else "🚀 LIVE MODE")
    print(f"📀 SQLite DB: {DB_PATH}")
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
          AND detail_fetched_at IS NULL
        ORDER BY last_seen DESC
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    if DRY_RUN:
        rows = rows[:DRY_RUN_LIMIT]

    if not rows:
        print("✅ Nothing to enrich")
        conn.close()
        return

    print(f"🔍 Rows selected: {len(rows)}")

    client = None
    if not DRY_RUN or DRY_RUN_FETCH_HTML:
        client = DealOpportunitiesClient()
        client.start()

    try:
        for r in rows:
            row_id = r["id"]
            deal_key = r["source_listing_id"]
            url = r["source_url"]

            print(f"\n➡️ Deal {deal_key}")

            # -----------------------------
            # FETCH DETAIL (OPTIONAL)
            # -----------------------------
            html = None
            if DRY_RUN and not DRY_RUN_FETCH_HTML:
                print("  🧪 Skipping HTML fetch")
            else:
                try:
                    pdf_path = PDF_ROOT / f"{deal_key}.pdf"
                    html = client.fetch_listing_detail_and_pdf(url, pdf_path)
                except PlaywrightError as e:
                    print(f"  ❌ Fetch failed: {e}")
                    continue

            if not html:
                continue

            title = extract_do_title(html) or r["title"]
            parsed = parse_do_detail(html)

            # -----------------------------
            # REVENUE INFERENCE (FROM IMPORTED TURNOVER)
            # -----------------------------
            if r["revenue_k"] is None:
                inferred = map_turnover_range_to_revenue_k(
                    r["turnover_range_raw"]
                )
                if inferred is not None:
                    if DRY_RUN:
                        print(
                            f"  🧪 WOULD SET revenue_k={inferred} "
                            f"(from turnover='{r['turnover_range_raw']}')"
                        )
                    else:
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

            # -----------------------------
            # SECTOR / INDUSTRY
            # -----------------------------
            raw_sector = parsed["facts"].get("sector")
            mapping = map_dealopportunities_sector(raw_sector=raw_sector)

            if DRY_RUN:
                print(
                    "  🧪 WOULD UPDATE:\n"
                    f"     title={title}\n"
                    f"     industry={mapping['industry']}\n"
                    f"     sector={mapping['sector']}\n"
                    f"     confidence={mapping['confidence']}"
                )
            else:
                content_hash = hashlib.sha256(html.encode()).hexdigest()
                fetched_at = datetime.utcnow().isoformat(timespec="seconds")

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
                        detail_fetched_at = ?,
                        needs_detail_refresh = 0,
                        detail_fetch_reason = NULL,
                        last_updated = CURRENT_TIMESTAMP
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
                        fetched_at,
                        row_id,
                    ),
                )

                conn.commit()
                pdf_path.unlink(missing_ok=True)

            print("  ✅ Done")

    finally:
        if client:
            client.stop()
        conn.close()

    print("\n🏁 DealOpportunities enrichment complete")


# =================================================
# ENTRYPOINT
# =================================================

if __name__ == "__main__":
    limit_env = os.getenv("ENRICH_LIMIT")
    limit = None if not limit_env else int(limit_env)
    enrich_dealopportunities(limit)