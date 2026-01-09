import hashlib
from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client


# -------------------------------------------------
# CONFIG
# -------------------------------------------------

SPREADSHEET_ID = "1PgEknRWYb50fPVbtqfCdA4UU3oTuwi8X-X-MGOIA2VU"

SHEETS = {
    "Aug25-D": "2025-08-01",
    "Oct25-D": "2025-10-01",
    "Dec25-D": "2025-12-01",
}

DRY_RUN = False


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def normalize_description(desc: str | None) -> str:
    if not desc:
        return ""
    return " ".join(
        desc.lower()
            .replace("\xa0", " ")
            .replace("\n", " ")
            .replace("\r", " ")
            .split()
    )

def hash_description(desc: str | None) -> str | None:
    norm_desc = normalize_description(desc)
    if not norm_desc:
        return None
    return hashlib.sha256(norm_desc.encode("utf-8")).hexdigest()

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


def map_interest_flag_to_decision(flag: str | None):
    if not flag:
        return None

    flag = flag.strip().upper()
    return {
        "NO": "Pass",
        "YES": "CIM",
        "MAYBE": "On-Hold (UOffer)",
    }.get(flag)


def derive_title_from_description(desc: str | None) -> str | None:
    if not desc:
        return None
    return desc.split("\n")[0].strip()[:200]


def fingerprint(sector_raw, location, revenue_k, ebitda_k):
    """
    Stable identity for Dmitry deals.
    Must never change once deployed.
    """
    key = "|".join([
        norm(sector_raw),
        norm(location),
        str(int(revenue_k)) if revenue_k is not None else "",
        str(int(ebitda_k)) if ebitda_k is not None else "",
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def parse_k_number(val):
    """
    Normalize numeric inputs from external sources.
    Returns float or None.
    """
    if val is None:
        return None

    if isinstance(val, (int, float)):
        return float(val)

    if not isinstance(val, str):
        return None

    v = (
        val.replace("£", "")
           .replace(",", "")
           .strip()
    )

    if v == "":
        return None

    try:
        return float(v)
    except ValueError:
        return None

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    now = datetime.utcnow().isoformat(timespec="seconds")

    imported = 0
    updated = 0

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

            revenue_raw = parse_money(row[5]) if len(row) > 5 else None
            ebitda_raw = parse_money(row[6]) if len(row) > 6 else None
            ebitda_margin = parse_pct(row[7]) if len(row) > 7 else None
            interest_flag = row[8] if len(row) > 8 else None

            revenue_k = round(revenue_raw / 1000, 1) if revenue_raw is not None else None
            ebitda_k = round(ebitda_raw / 1000, 1) if ebitda_raw is not None else None

            # IMPORTANT:
            # Dmitry deals use description_hash as identity.
            # Do NOT change this without a full delete + reimport.
            source_listing_id = hash_description(description)

            decision = map_interest_flag_to_decision(interest_flag)

            deal = {

                "description": description,
                "description_hash": hash_description(description),
                "description_len": len(description) if description else None,

                # -------------------------
                # Identity
                # -------------------------
                "source": "Dmitry",
                "source_listing_id": source_listing_id,
                "source_url": "dmitry-sheet",

                # -------------------------
                # Core descriptors
                # -------------------------
                "title": derive_title_from_description(description),
                "industry": None,
                "sector": None,
                "location": location,
                "incorporation_year": None,

                # -------------------------
                # Financials
                # -------------------------
                "revenue_k": revenue_k,
                "ebitda_k": ebitda_k,
                "asking_price_k": None,
                "ebitda_margin": ebitda_margin,
                "revenue_growth_pct": None,
                "leverage_pct": None,

                # -------------------------
                # Workflow / decisioning
                # -------------------------
                "status": None,        # never touched by importers
                "owner": None,
                "priority": None,
                "notes": None,
                "decision": decision,
                "decision_reason": None,

                # -------------------------
                # Lifecycle
                # -------------------------
                "first_seen": sheet_date,
                "last_seen": sheet_date,
                "last_updated": now,
                "last_updated_source": "AUTO",

                # -------------------------
                # Assets
                # -------------------------
                "drive_folder_url": None,
            }

            if DRY_RUN:
                print("DRY:", deal)
                continue

            existing = repo.fetch_by_source_and_listing(
                source="Dmitry",
                source_listing_id=source_listing_id,
            )

            desc_hash = deal["description_hash"]

            if desc_hash:
                dup = repo.fetch_all(
                    """
                    SELECT source_listing_id
                    FROM deals
                    WHERE source = 'Dmitry'
                      AND description_hash = ?
                      AND source_listing_id != ?
                    LIMIT 1
                    """,
                    (desc_hash, source_listing_id),
                )

                if dup:
                    deal["notes"] = (
                            (deal.get("notes") or "")
                            + f"\n⚠ Possible duplicate of {dup['source_listing_id']} (desc hash match)"
                    )

            repo.upsert_deal_v2(deal)

            if existing:
                updated += 1
            else:
                imported += 1

    print(
        f"✅ Dmitry import complete — imported={imported}, updated={updated}"
    )


if __name__ == "__main__":
    main()