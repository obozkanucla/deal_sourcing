import hashlib
from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client


SPREADSHEET_ID = "1PgEknRWYb50fPVbtqfCdA4UU3oTuwi8X-X-MGOIA2VU"

SHEETS = {
    "Aug25-D": "2025-08-01",
    "Oct25-D": "2025-10-01",
    "Dec25-D": "2025-12-01",
}

DRY_RUN = False


# -------------------------
# Helpers
# -------------------------

def norm(x):
    return str(x or "").strip().lower()


def parse_money(val):
    try:
        return float(
            str(val)
            .replace(",", "")
            .replace("£", "")
            .replace("$", "")
            .replace("€", "")
            .strip()
        )
    except Exception:
        return None


def parse_pct(val):
    try:
        return float(str(val).replace("%", "").strip())
    except Exception:
        return None


def fingerprint(sector_raw, location, revenue, ebitda):
    """
    Stable identity across sheets.
    DO NOT include description.
    """
    key = "|".join([
        norm(sector_raw),
        norm(location),
        str(int(revenue)) if revenue is not None else "",
        str(int(ebitda)) if ebitda is not None else "",
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


# -------------------------
# Main
# -------------------------

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    now = datetime.utcnow().isoformat()
    imported, updated = 0, 0

    for sheet_name, sheet_date in SHEETS.items():
        ws = sh.worksheet(sheet_name)
        values = ws.get_all_values()
        rows = values[1:]  # skip header

        for row in rows:
            if len(row) < 4:
                continue

            sector_raw = row[0]
            location = row[1]
            description = row[2]

            revenue = parse_money(row[3])
            ebitda = parse_money(row[4]) if len(row) > 4 else None
            ebitda_margin = parse_pct(row[7]) if len(row) > 7 else None
            interest_flag = row[8] if len(row) > 8 else None

            deal_fp = fingerprint(sector_raw, location, revenue, ebitda)
            deal_id = f"Dmitry:{deal_fp}"

            deal = {
                "deal_id": deal_id,
                "source": "Dmitry",
                "intermediary": "Dmitry Mykhailenko",
                "source_listing_id": deal_fp,   # ✅ stable
                "source_url": "dmitry-sheet",
                "identity_method": "dmitry_fingerprint",
                "content_hash": deal_fp,
                "description": description,
                "sector_raw": sector_raw,
                "industry_raw": None,
                "location": location,
                "revenue_latest": revenue,
                "ebitda_latest": ebitda,
                "ebitda_margin_pct": ebitda_margin,
                "notes": f"interest_flag={interest_flag}" if interest_flag else None,
                "first_seen": sheet_date,
                "last_seen": sheet_date,
                "manual_imported_at": now,
            }

            if DRY_RUN:
                print(deal_id, sector_raw)
                continue

            existing = repo.fetch_by_deal_id(deal_id)

            if not existing:
                repo.insert_raw_deal(deal)
                imported += 1
            else:
                repo.upsert_dmitry_seen(
                    deal_id=deal_id,
                    first_seen=min(existing["first_seen"], sheet_date),
                    last_seen=max(existing["last_seen"], sheet_date),
                )
                updated += 1

    print(f"✅ Dmitry import complete — imported={imported}, updated={updated}")


if __name__ == "__main__":
    main()