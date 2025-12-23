import json
import re
from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client


# =========================
# CONFIG
# =========================

SPREADSHEET_ID = "1PgEknRWYb50fPVbtqfCdA4UU3oTuwi8X-X-MGOIA2VU"
WORKSHEET_NAME = "Deals"

DRY_RUN = False   # ← set to False to write to SQLite


# =========================
# HELPERS
# =========================

def slugify(text: str) -> str:
    if not text:
        return "unknown"
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text.strip("-")


def parse_date(val: str):
    if not val:
        return None
    try:
        return datetime.strptime(val.strip(), "%m/%d/%Y").date().isoformat()
    except Exception:
        return None


def parse_float(val):
    try:
        if val in ("", None):
            return None
        return float(val)
    except Exception:
        return None


def parse_int(val):
    try:
        if val in ("", None):
            return None
        return int(float(val))
    except Exception:
        return None


# =========================
# MAIN IMPORT LOGIC
# =========================

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    raw_values = ws.get_all_values()
    headers = raw_values[0]
    rows = raw_values[1:]

    col_idx = {h.strip(): i for i, h in enumerate(headers)}

    print(f"📥 Loaded {len(rows)} legacy rows")

    imported = 0

    for row_num, row in enumerate(rows, start=2):

        def val(col):
            idx = col_idx.get(col)
            if idx is None or idx >= len(row):
                return ""
            return row[idx].strip()

        company = val("Company Name")
        intermediary = val("Broker / Source")

        if not company:
            continue

        deal_id = f"Legacy:{slugify(intermediary)}:{slugify(company)}"

        deal = {
            "deal_id": deal_id,
            "source": "LegacySheet",
            "intermediary": intermediary,
            "company_name": company,
            "industry": val("Industry"),
            "sector": val("Sector"),
            "sector_source": "manual",
            "location": val("Location"),
            "incorporation_year": parse_int(val("Inc")),
            "first_seen": parse_date(val("Date Received")),
            "last_updated": parse_date(val("Last update")),
            "outcome": val("Outcome"),
            "outcome_reason": val("Reason"),
            "notes": val("Update"),
            "revenue_k": parse_float(val("Latest revenue (annual-000)")),
            "ebitda_k": parse_float(val("EBITDA (Latest)")),
            "ebitda_margin": parse_float(val("EBITDA Margin %")),
            "asking_price_k": parse_float(val("Asking Price / Valuation")),
            "revenue_multiple": parse_float(val("Revenue Multiple")),
            "ebitda_multiple": parse_float(val("EBITDA Multiple")),
            "drive_folder_url": val("G-Link URL"),  # ✅ FIXED
        }

        if DRY_RUN:
            print(f"🧪 DRY RUN: {deal_id}")
            print(json.dumps(deal, indent=2))
            continue

        repo.upsert_legacy_deal(deal)
        imported += 1

    if DRY_RUN:
        print("🧪 DRY RUN complete — no data written")
    else:
        print(f"✅ Imported {imported} legacy deals into SQLite")


if __name__ == "__main__":
    main()