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
        "YES": "Proceed",
        "MAYBE": "Park",
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
import time
from gspread.exceptions import APIError

def get_all_values_with_retry(ws, retries=5, base_delay=2):
    for attempt in range(1, retries + 1):
        try:
            return ws.get_all_values()
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status != 503:
                raise
            if attempt == retries:
                raise
            sleep = base_delay * attempt
            print(f"⚠️ Google Sheets 503, retry {attempt}/{retries} in {sleep}s")
            time.sleep(sleep)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    now = datetime.today().isoformat()

    imported = 0
    updated = 0

    for sheet_name, sheet_date in SHEETS.items():
        ws = sh.worksheet(sheet_name)
        values = get_all_values_with_retry(ws)

        if not values or len(values) < 2:
            print(f"⚠️ Sheet {sheet_name} is empty or malformed, skipping")
            continue
        headers = values[0]
        rows = values[1:]

        # Header → index mapping (case-insensitive)
        idx = {h.strip().lower(): i for i, h in enumerate(headers)}
        REQUIRED_HEADERS = {
            "description",
            "region",
            "revenue - clean",
            "ebitda - clean",
        }

        missing = REQUIRED_HEADERS - idx.keys()
        if missing:
            raise RuntimeError(f"Dmitry sheet missing columns: {missing}")

        for row in rows:
            if len(row) < 3:
                continue

            location = row[idx["region"]] if "region" in idx else None
            description = row[idx["description"]] if "description" in idx else None

            # ✅ USE CLEAN NUMERIC COLUMNS ONLY
            revenue_raw = (
                parse_money(row[idx["revenue - clean"]])
                if "revenue - clean" in idx and idx["revenue - clean"] < len(row)
                else None
            )

            ebitda_raw = (
                parse_money(row[idx["ebitda - clean"]])
                if "ebitda - clean" in idx and idx["ebitda - clean"] < len(row)
                else None
            )

            # ✅ DERIVE EBITDA MARGIN (ignore sheet % column)
            ebitda_margin = (
                round((ebitda_raw / revenue_raw) * 100, 2)
                if revenue_raw and ebitda_raw
                else None
            )

            INTEREST_HEADERS = {
                "interest",
                "interested",
                "interest?",
                "decision",
                "Interested" # sometimes reused in Dmitry sheets
            }

            interest_col = next(
                (idx[h] for h in INTEREST_HEADERS if h in idx),
                None
            )

            interest_flag = (
                row[interest_col].strip()
                if interest_col is not None and interest_col < len(row)
                else None
            )

            revenue_k = round(revenue_raw / 1000, 1) if revenue_raw is not None else None
            ebitda_k = round(ebitda_raw / 1000, 1) if ebitda_raw is not None else None

            # 🔑 DESCRIPTION HASH = IDENTITY
            # IMPORTANT:
            # Dmitry deals use description_hash as identity.
            # Do NOT change this without a full delete + reimport.
            source_listing_id = hash_description(description)
            decision = map_interest_flag_to_decision(interest_flag)
            existing = repo.fetch_by_source_and_listing(
                source="Dmitry",
                source_listing_id=source_listing_id,
            )

            # ---------------------------------
            # Decision downgrade protection
            # ---------------------------------
            if (
                    decision is None
                    and existing
                    and existing.get("decision") is not None
            ):
                decision = existing.get("decision")

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