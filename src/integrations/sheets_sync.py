import time


SHEET_COLUMNS = [
    "deal_id",
    "source",
    "source_listing_id",
    "source_url",
    "sector",
    "status",
    "owner",
    "priority",
    "notes",
    "last_touch",
    "first_seen",
    "last_seen",
    "last_updated",
    "decision",
    "decision_confidence",
    "pdf_link",
]


# -----------------------------
# Helpers
# -----------------------------

def get_existing_deal_ids(ws) -> set:
    """
    Read column A (deal_id) once.
    """
    values = ws.col_values(1)
    return set(v.strip() for v in values[1:] if v.strip())


def row_from_deal(deal: dict) -> list:
    """
    Build a sheet row in exact column order.
    Human-editable fields intentionally start empty.
    """
    return [
        deal["deal_id"],
        deal.get("source"),
        deal.get("source_listing_id"),
        deal.get("source_url"),
        deal.get("sector"),
        "",  # status
        "",  # owner
        "",  # priority
        "",  # notes
        "",  # last_touch
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
            rows[i:i + chunk_size],
            value_input_option="USER_ENTERED",
        )
        print(f"✅ Appended {len(rows[i:i + chunk_size])} rows")
        time.sleep(1)


# -----------------------------
# Main entry
# -----------------------------

def push_sqlite_to_sheets(repo, ws):
    deals = repo.fetch_all_deals()
    print(f"📦 Loaded {len(deals)} deals from SQLite")

    existing = get_existing_deal_ids(ws)
    print(f"📄 Sheet already has {len(existing)} deals")

    rows = []
    for deal in deals:
        if deal["deal_id"] in existing:
            continue
        rows.append(row_from_deal(deal))


    print(f"🆕 {len(rows)} new deals to export")

    if rows:
        append_rows(ws, rows)
    else:
        print("ℹ️ Nothing new to push")

def pull_sheets_to_sqlite(repo, worksheet):
    rows = worksheet.get_all_records()

    print(f"📥 Loaded {len(rows)} rows from Google Sheets")

    editable_fields = {
        "status",
        "owner",
        "priority",
        "notes",
        "last_touch",
        "decision",
        "decision_confidence",
    }

    updated = 0
    skipped = 0

    for row in rows:
        deal_id = row.get("deal_id")
        if not deal_id:
            continue

        db_deal = repo.fetch_by_deal_id(deal_id)
        if not db_deal:
            continue  # safety: don’t create deals from Sheets

        updates = {}

        for field in editable_fields:
            sheet_val = row.get(field)
            db_val = db_deal.get(field)

            # normalize empty strings
            if sheet_val == "":
                sheet_val = None

            if sheet_val != db_val:
                updates[field] = sheet_val

        if updates:
            repo.update_deal_fields(deal_id, updates)
            updated += 1
            print(f"🔄 Updated {deal_id}: {updates}")
        else:
            skipped += 1

    print(f"✅ Reverse sync complete — {updated} updated, {skipped} unchanged")

def update_folder_links(repo, ws):
    """
    Update folder links for existing rows based on deal_id.
    """
    records = ws.get_all_records()
    print(f"🔄 Checking {len(records)} existing rows for folder links")

    updates = 0

    for i, row in enumerate(records, start=2):  # row 1 = header
        deal_id = row.get("deal_id")
        if not deal_id:
            continue

        # Skip if already populated
        if row.get("pdf_link"):
            continue

        deal = repo.fetch_by_deal_id(deal_id)
        if not deal:
            continue

        folder_url = deal.get("drive_folder_url")
        if not folder_url:
            continue

        ws.update_cell(
            i,
            SHEET_COLUMNS.index("pdf_link") + 1,
            f'=HYPERLINK("{folder_url}", "Folder")',
        )
        updates += 1

        if updates % 20 == 0:
            time.sleep(1)  # avoid quota issues

    print(f"✅ Updated {updates} folder links")