import re
from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client


# =========================
# CONFIG
# =========================

SPREADSHEET_ID = "1PgEknRWYb50fPVbtqfCdA4UU3oTuwi8X-X-MGOIA2VU"
WORKSHEET_NAME = "Brokers et al"   # exact tab name

DRY_RUN = False   # ← set False to write


# =========================
# HELPERS
# =========================

def slugify(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text.strip("-")


def parse_bool(val):
    if not val:
        return None
    v = val.strip().lower()
    if v in ("yes", "y", "true", "1"):
        return 1
    if v in ("no", "n", "false", "0", "x"):
        return 0
    return None


def parse_date(val):
    if not val:
        return None
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date().isoformat()
        except Exception:
            pass
    return None


# =========================
# MAIN
# =========================

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    values = ws.get_all_values()
    headers = values[0]
    rows = values[1:]

    col_idx = {h.strip(): i for i, h in enumerate(headers)}

    def val(row, col):
        idx = col_idx.get(col)
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    print(f"📥 Loaded {len(rows)} intermediaries")

    imported = 0

    for row in rows:
        name = val(row, "Brokers")
        if not name:
            continue

        intermediary_id = slugify(name)

        record = {
            "intermediary_id": intermediary_id,
            "name": name,
            "website": val(row, "Website"),
            "last_checked": parse_date(val(row, "Last checked")),
            "existing_relationship": parse_bool(val(row, "Existing Rel.")),
            "relationship_owner": val(row, "WHO"),
            "active": parse_bool(val(row, "Active")),
            "sector_focus": val(row, "Sector"),
            "geography": val(row, "Geography"),
            "category": val(row, "Category"),
            "notes": val(row, "Note"),
            "description": val(row, "Description"),
        }

        if DRY_RUN:
            print(f"🧪 DRY RUN: {intermediary_id}")
            print(record)
            continue

        repo.upsert_intermediary(record)
        imported += 1

    if DRY_RUN:
        print("🧪 DRY RUN complete — no data written")
    else:
        print(f"✅ Imported {imported} intermediaries")


if __name__ == "__main__":
    main()