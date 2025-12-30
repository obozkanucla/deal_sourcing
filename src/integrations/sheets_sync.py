import time
from typing import Iterable, Set, Tuple
from src.domain.deal_columns import DEAL_COLUMNS
from gspread.utils import rowcol_to_a1

SHEET_COLUMNS = [c.name for c in DEAL_COLUMNS]
ALLOWED_COLUMNS = {c.name for c in DEAL_COLUMNS if c.push or c.pull}
SYSTEM_FIELDS = {c.name for c in DEAL_COLUMNS if c.system}

# -----------------------------
# Helpers
# -----------------------------

def get_existing_deal_ids(ws) -> set[str]:
    values = ws.col_values(1)  # Column A = deal_uid
    return {v.strip() for v in values[1:] if v.strip()}


def row_from_deal(deal: dict) -> list:
    deal_uid = f"{deal['source']}:{deal['source_listing_id']}"

    return [
        deal_uid,
        deal.get("source"),
        deal.get("source_listing_id"),
        deal.get("source_url"),
        deal.get("title"),
        deal.get("industry"),
        deal.get("sector"),
        deal.get("location"),
        deal.get("status"),
        deal.get("owner"),
        deal.get("priority"),
        deal.get("notes"),
        deal.get("last_touch"),
        deal.get("first_seen"),
        deal.get("last_seen"),
        deal.get("last_updated"),
        deal.get("decision"),
        deal.get("decision_confidence"),
        (
            f'=HYPERLINK("{deal["drive_folder_url"]}", "Folder")'
            if deal.get("drive_folder_url")
            else ""
        ),
    ]

def append_rows(ws, rows, chunk_size=200):
    for i in range(0, len(rows), chunk_size):
        ws.append_rows(
            rows[i : i + chunk_size],
            value_input_option="USER_ENTERED",
        )
        print(f"✅ Appended {len(rows[i : i + chunk_size])} rows")
        time.sleep(1)


# -----------------------------
# PUSH: SQLite → Sheets
# -----------------------------

def push_sqlite_to_sheets(
    repo,
    ws,
    *,
    allowed_columns: Set[str]
):
    if not allowed_columns:
        raise ValueError("allowed_columns must be explicitly provided")

    deals = repo.fetch_all_deals()
    print(f"📦 Loaded {len(deals)} deals from SQLite")

    existing = get_existing_deal_ids(ws)
    print(f"📄 Sheet already has {len(existing)} deals")

    rows = []
    for deal in deals:
        deal_uid = f"{deal['source']}:{deal['source_listing_id']}"
        if deal_uid in existing:
            continue
        rows.append(row_from_deal(deal))

    print(f"🆕 {len(rows)} new deals to export")

    if rows:
        append_rows(ws, rows)
    else:
        print("ℹ️ Nothing new to push")


# -----------------------------
# PULL: Sheets → SQLite
# -----------------------------

def pull_sheets_to_sqlite(
    repo,
    ws,
    *,
    allowed_columns: Set[str]
):
    if not allowed_columns:
        raise ValueError("allowed_columns must be explicitly provided")

    rows = ws.get_all_records()
    print(f"📥 Loaded {len(rows)} rows from Google Sheets")

    updated = 0
    skipped = 0

    for row in rows:
        deal_uid = row.get("deal_uid")
        if not deal_uid:
            continue

        try:
            source, source_listing_id = deal_uid.split(":", 1)
        except ValueError:
            continue # malformed UID

        db_deal = repo.fetch_by_source_and_listing(source, source_listing_id)
        if not db_deal:
            continue  # never create from Sheets

        updates = {}

        for field in allowed_columns:
            if field in SYSTEM_FIELDS:
                continue  # 🚫 never pull these from Sheets
            sheet_val = row.get(field)
            db_val = db_deal.get(field)

            if sheet_val == "":
                continue
            if sheet_val != db_val:
                updates[field] = sheet_val

        if updates:
            repo.update_deal_fields(
                db_deal["id"],  # ← SQLite primary key
                updates
            )
            updated += 1
            print(f"🔄 Updated {deal_uid}: {updates}")
        else:
            skipped += 1

    print(f"✅ Reverse sync complete — {updated} updated, {skipped} unchanged")


# -----------------------------
# PATCH: Folder links only
# -----------------------------

import time

def update_folder_links(repo, ws):
    """
    Backfill Drive Folder links in Google Sheets
    using deal_uid (source:source_listing_id).
    """

    values = ws.get_all_values()
    if not values:
        print("⚠️ Sheet is empty")
        return

    headers = values[0]
    rows = values[1:]

    print(f"🔄 Checking {len(rows)} rows for missing Drive Folder links")

    try:
        deal_uid_col = headers.index("deal_uid")
        folder_col = headers.index("drive_folder_url")
    except ValueError as e:
        raise RuntimeError(
            f"Required column missing in sheet headers. Found: {headers}"
        ) from e

    updates = 0

    for row_idx, row in enumerate(rows, start=2):  # header = row 1
        deal_uid = row[deal_uid_col].strip() if deal_uid_col < len(row) else ""
        current_link = row[folder_col].strip() if folder_col < len(row) else ""

        if not deal_uid or current_link:
            continue

        # deal_uid = "Source:ListingID"
        try:
            source, source_listing_id = deal_uid.split(":", 1)
        except ValueError:
            continue  # malformed UID, skip safely

        deal = repo.fetch_by_source_and_listing(source, source_listing_id)
        if not deal:
            continue

        folder_url = deal.get("drive_folder_url")
        if not folder_url:
            continue

        ws.update_cell(
            row_idx,
            folder_col + 1,  # Sheets are 1-indexed
            f'=HYPERLINK("{folder_url}", "Folder")',
        )

        updates += 1

        if updates % 20 == 0:
            time.sleep(1)

    print(f"✅ Updated {updates} Drive Folder links")

def backfill_system_columns(repo, ws, columns, batch_size=100):
    """
    Backfill system-derived columns (title, industry, sector, location)
    without overwriting existing sheet values.
    Uses batch updates to avoid API quota issues.
    """

    values = ws.get_all_values()
    if not values:
        print("⚠️ Sheet is empty")
        return

    headers = values[0]
    rows = values[1:]
    header_idx = {h: i for i, h in enumerate(headers)}

    updates = []

    for row_offset, row in enumerate(rows):
        row_num = row_offset + 2  # sheet rows start at 1, header is row 1

        deal_uid = row[header_idx["deal_uid"]].strip()
        if not deal_uid:
            continue

        try:
            source, source_listing_id = deal_uid.split(":", 1)
        except ValueError:
            continue

        deal = repo.fetch_by_source_and_listing(source, source_listing_id)
        if not deal:
            continue

        for col in columns:
            col_idx = header_idx.get(col)
            if col_idx is None:
                continue

            # do not overwrite existing values
            if col_idx < len(row) and row[col_idx].strip():
                continue

            val = deal.get(col)
            if not val:
                continue

            updates.append({
                "range": rowcol_to_a1(row_num, col_idx + 1),
                "values": [[val]],
            })

        # Flush in batches
        if len(updates) >= batch_size:
            ws.batch_update(updates)
            updates.clear()
            time.sleep(1)  # stay well under quota

    # Flush remaining updates
    if updates:
        ws.batch_update(updates)

    print("✅ System column backfill complete")