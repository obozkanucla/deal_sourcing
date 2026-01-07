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

import re

def parse_money_k(val: str | None) -> float | None:
    if not val:
        return None

    s = val.lower().strip()

    # hard stop tokens
    if any(x in s for x in ("poa", "na", "#div")):
        return None

    s = s.replace("£", "").replace(",", "").strip()

    multiplier = 1.0
    if "m" in s:
        multiplier = 1000.0
        s = s.replace("m", "")
    elif "k" in s:
        s = s.replace("k", "")

    match = re.search(r"\d+(\.\d+)?", s)
    if not match:
        return None

    return float(match.group()) * multiplier



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
        industry = val("Industry")

        sector = (
                val("Sector")
                or val("Sector #1")
                or val("Sector #2")
                or val("Sector #3")
                or val("Sector #4")
                or None
        )
        outcome = val("Outcome")
        outcome_reason = val("Reason")
        legacy_notes = val("Update")

        notes_parts = []
        if intermediary:
            notes_parts.append(f"Intermediary: {intermediary}")
        if legacy_notes:
            notes_parts.append(legacy_notes)

        deal = {
            "deal_id": deal_id,
            "source": "LegacySheet",
            # ✅ canonical title
            "title": company,
            "industry": industry,
            "sector": sector,
            "sector_source": "manual",
            "location": val("Location"),
            "incorporation_year": parse_int(val("Inc")),
            "first_seen": parse_date(val("Date Received")),
            "last_updated": parse_date(val("Last update")),
            "outcome": outcome,  # → status
            "outcome_reason": outcome_reason,
            "status": val("Outcome"),
            "notes": "\n".join(notes_parts) if notes_parts else None,
            "revenue_k": parse_money_k(val("Latest revenue (annual-000)")),
            "ebitda_k": parse_money_k(val("EBITDA (Latest)")),
            "asking_price_k": parse_money_k(val("Asking Price / Valuation")),
            "drive_folder_url": val("G-Link URL"),  # ✅ FIXED
            "decision": None,
            "decision_reason": val("Reason"),
        }

        if DRY_RUN:
            print(f"🧪 DRY RUN: {deal_id}")
            print(json.dumps(deal, indent=2))
            continue

        # print(
        #     deal["deal_id"],
        #     "OUTCOME=", repr(deal.get("outcome"))
        # )
        repo.upsert_legacy_deal(deal)
        imported += 1

    if DRY_RUN:
        print("🧪 DRY RUN complete — no data written")
    else:
        print(f"✅ Imported {imported} legacy deals into SQLite")


if __name__ == "__main__":
    main()