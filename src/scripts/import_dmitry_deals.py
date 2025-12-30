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

def map_interest_flag_to_decision(flag: str | None):
    if not flag:
        return None

    flag = flag.strip().upper()

    return {
        "NO": "Pass",
        "YES": "CIM",
        "MAYBE": "Parked",
    }.get(flag)

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
            revenue_raw = parse_money(row[3])
            ebitda_raw = parse_money(row[4]) if len(row) > 4 else None

            revenue = round(revenue_raw / 1000, 1) if revenue_raw is not None else None
            ebitda = round(ebitda_raw / 1000, 1) if ebitda_raw is not None else None
            ebitda_margin = parse_pct(row[7]) if len(row) > 7 else None
            interest_flag = row[8] if len(row) > 8 else None

            deal_fp = fingerprint(sector_raw, location, revenue, ebitda)
            deal_id = f"Dmitry:{deal_fp}"

            decision = map_interest_flag_to_decision(interest_flag)

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
                "revenue_k": revenue,
                "ebitda_k": ebitda,
                "asking_price_k": None,
                # ✅ canonical decision
                "decision": decision,
                "decision_reason": None,
                "status": None,  # ❌ never touch
                # optional provenance (debuggable, not operational)
                "notes": None,
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
                # 1️⃣ Enrich missing fields (safe, COALESCE)
                repo.update_dmitry_enrichment(
                    deal_id=deal_id,
                    revenue_k=revenue,
                    ebitda_k=ebitda,
                    decision=decision,
                    notes=None,
                )

                # 2️⃣ Update seen dates
                repo.upsert_dmitry_seen(
                    deal_id=deal_id,
                    first_seen=min(existing["first_seen"], sheet_date),
                    last_seen=max(existing["last_seen"], sheet_date),
                )

                updated += 1

    print(f"✅ Dmitry import complete — imported={imported}, updated={updated}")


if __name__ == "__main__":
    main()