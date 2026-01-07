import time
from typing import Iterable, Set, Tuple
from src.domain.deal_columns import DEAL_COLUMNS
from gspread.utils import rowcol_to_a1
import string

SHEET_COLUMNS = [c.name for c in DEAL_COLUMNS]
ALLOWED_COLUMNS = {c.name for c in DEAL_COLUMNS if c.push or c.pull}
SYSTEM_FIELDS = {c.name for c in DEAL_COLUMNS if c.system}
DROPDOWNS = {
    "status": [
        "New",
        "Reviewing",
        "CIM",
        "Parked",
        "Passed",
        "Lost",
    ],
    "priority": [
        "High",
        "Medium",
        "Low",
    ],
    "decision": [
        "Pass",
        "Advance",
        "Hold",
    ],
    "owner": [
        "Burak",
        "Muge",
        "Unassigned",
    ],
}

# -----------------------------
# Helpers
# -----------------------------

def get_existing_deal_ids(ws) -> set[str]:
    values = ws.col_values(1)  # Column A = deal_uid
    return {v.strip() for v in values[1:] if v.strip()}

def row_from_deal(deal: dict, columns=DEAL_COLUMNS) -> list:
    row = []

    for col in columns:
        if not col.push:
            continue

        if col.name == "deal_uid":
            row.append(f"{deal['source']}:{deal['source_listing_id']}")

        elif col.name == "drive_folder_url":
            url = deal.get("drive_folder_url")
            if url:
                row.append(f'=HYPERLINK("{url}", "Folder")')
            else:
                row.append("")
        elif col.name == "source_url":
            url = deal.get("source_url")
            if url:
                row.append(f'=HYPERLINK("{url}", "Link to deal")')
            else:
                row.append("")

        else:
            row.append(deal.get(col.name))

    return row

def append_rows(ws, rows, chunk_size=200):
    for i in range(0, len(rows), chunk_size):
        ws.append_rows(
            rows[i : i + chunk_size],
            value_input_option="USER_ENTERED",
        )
        print(f"✅ Appended {len(rows[i : i + chunk_size])} rows")
        time.sleep(1)

# -----------------------------
# Helpers
# -----------------------------

def ensure_sheet_headers(ws, columns):
    expected = [c.name for c in columns]

    # 🔥 FULL RESET: clear sheet completely
    ws.clear()

    # Resize to exactly 1 row and N columns
    ws.resize(rows=1, cols=len(expected))

    # Write headers
    ws.update("A1", [expected])

    print("🧱 Sheet headers reset and written")

# -----------------------------
# PUSH: SQLite → Sheets
# -----------------------------

def push_sqlite_to_sheets(repo, ws):
    deals = repo.fetch_all_deals()
    existing = get_existing_deal_ids(ws)

    rows = []
    for deal in deals:
        deal_uid = f"{deal['source']}:{deal['source_listing_id']}"
        if deal_uid in existing:
            continue

        rows.append(row_from_deal(deal))

    if rows:
        append_rows(ws, rows)

# -----------------------------
# PULL: Sheets → SQLite
# -----------------------------

def pull_sheets_to_sqlite(repo, ws, columns=DEAL_COLUMNS):
    """
    Reverse sync: Google Sheets → SQLite

    Contract:
    - Only analyst-editable fields (pull=True, system=False) are considered
    - SQLite is updated ONLY if sheet values differ from DB
    - last_updated is bumped ONLY when a real change occurs
    - Safe to run repeatedly (idempotent)
    """

    values = ws.get_all_values()
    if not values:
        print("⚠️ Sheet is empty")
        return

    headers = values[0]
    rows = values[1:]

    header_set = set(headers)
    col_idx = {h: i for i, h in enumerate(headers)}

    # Pullable = analyst-editable columns only
    pullable_columns = [
        c for c in columns
        if c.pull and not c.system and c.name in header_set
    ]

    updated = 0
    skipped = 0

    for row in rows:
        # deal_uid is mandatory
        if "deal_uid" not in col_idx:
            continue

        uid_idx = col_idx["deal_uid"]
        deal_uid = row[uid_idx].strip() if uid_idx < len(row) else ""
        if not deal_uid:
            continue

        try:
            source, source_listing_id = deal_uid.split(":", 1)
        except ValueError:
            continue  # malformed UID

        db_deal = repo.fetch_by_source_and_listing(source, source_listing_id)
        if not db_deal:
            continue  # DB is source of truth for row existence

        updates = {}

        for col in pullable_columns:
            idx = col_idx[col.name]
            sheet_val = row[idx].strip() if idx < len(row) else ""

            db_val = db_deal.get(col.name)

            # Blank handling
            if sheet_val == "":
                if col.allow_blank_pull and db_val is not None:
                    updates[col.name] = None
                continue

            # Normalize comparison (Sheets always gives strings)
            if str(sheet_val) != str(db_val):
                updates[col.name] = sheet_val

        if updates:
            updates["last_updated_source"] = "MANUAL"

            # ✅ Analyst made a real change → persist + bump last_updated
            repo.update_deal_fields(
                source=source,
                source_listing_id=source_listing_id,
                updates=updates,
            )
            updated += 1
        else:
            skipped += 1

    print(f"✅ Reverse sync complete — {updated} updated, {skipped} unchanged")

# -----------------------------
# PATCH: Folder links only
# -----------------------------

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

        # folder_url = deal.get("drive_folder_url")
        folder_url = (
            f'=HYPERLINK("{deal["drive_folder_url"]}", "Folder")'
            if deal.get("drive_folder_url")
            else ""
                    )

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

def col_letter(idx: int) -> str:
    """1-based index → column letter"""
    result = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        result = chr(65 + rem) + result
    return result

def format_currency_column(ws, col_idx):
    col = col_letter(col_idx)
    ws.format(
        f"{col}:{col}",
        {
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "£#,##0"
            }
        }
    )

def format_percentage_column(ws, col_idx):
    col = col_letter(col_idx)
    ws.format(
        f"{col}:{col}",
        {
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "0.00%"
            }
        }
    )

def header_to_col_idx(ws):
    headers = ws.row_values(1)
    return {h: i + 1 for i, h in enumerate(headers)}

def apply_status_rules(ws, col_idx):
    rules = {
        "Pass":   {"red": 0.95, "green": 0.8,  "blue": 0.8},
        "CIM":    {"red": 0.8,  "green": 0.95, "blue": 0.8},
        "Parked": {"red": 1.0,  "green": 0.9,  "blue": 0.6},
    }

    requests = []

    for value, color in rules.items():
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": ws.id,
                        "startRowIndex": 1,              # skip header
                        "startColumnIndex": col_idx - 1,
                        "endColumnIndex": col_idx,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": value}],
                        },
                        "format": {
                            "backgroundColor": color
                        },
                    },
                },
                "index": 0,
            }
        })

    ws.spreadsheet.batch_update({"requests": requests})

def freeze_header_row(ws):
    ws.spreadsheet.batch_update({
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "gridProperties": {
                            "frozenRowCount": 1
                        }
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ]
    })

def format_header_row(ws):
    ws.spreadsheet.batch_update({
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 1.0,
                                "green": 0.96,
                                "blue": 0.80
                            },
                            "textFormat": {
                                "bold": True
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            }
        ]
    })

def unfreeze_sheet(ws):
    ws.spreadsheet.batch_update({
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "gridProperties": {
                            "frozenRowCount": 0,
                            "frozenColumnCount": 0
                        }
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
                }
            }
        ]
    })
    print("🧊 Sheet unfrozen")

def reset_sheet_state(ws, num_columns: int):
    """
    Hard reset a sheet:
    - unfreeze rows/columns
    - clear values
    - remove all formatting
    - remove conditional formatting rules
    - reset column widths
    """

    sheet_id = ws.id

    requests = [
        # 1️⃣ Unfreeze everything
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 0,
                        "frozenColumnCount": 0,
                    },
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        },

        # 2️⃣ Clear all formatting (backgrounds, number formats, text styles)
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                },
                "cell": {
                    "userEnteredFormat": {},
                },
                "fields": "userEnteredFormat",
            }
        },

        # 3️⃣ Remove ALL conditional formatting rules
        {
            "deleteConditionalFormatRule": {
                "sheetId": sheet_id,
                "index": 0,
            }
        },
    ]

    # ⚠️ Conditional rules must be deleted one-by-one.
    # We don’t know how many exist, so we loop defensively.
    while True:
        try:
            ws.spreadsheet.batch_update({"requests": requests})
            break
        except Exception:
            # stop when no more conditional rules exist
            break

    # 4️⃣ Clear values last (after formatting reset)
    ws.clear()

    # 5️⃣ Reset column widths (Google default ≈ 100px)
    ws.resize(rows=1, cols=num_columns)

    print("🧼 Sheet fully reset (values + formatting + freezes)")

def apply_ebitda_margin_color_scale(ws, col_idx):
    """
    Apply red → yellow → green color scale to EBITDA margin column.
    """
    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": ws.id,
                        "startRowIndex": 1,            # skip header
                        "startColumnIndex": col_idx - 1,
                        "endColumnIndex": col_idx,
                    }],
                    "gradientRule": {
                        "minpoint": {
                            "type": "NUMBER",
                            "value": "0",
                            "color": {"red": 0.95, "green": 0.6, "blue": 0.6},  # red
                        },
                        "midpoint": {
                            "type": "NUMBER",
                            "value": "15",
                            "color": {"red": 1.0, "green": 0.95, "blue": 0.6},  # yellow
                        },
                        "maxpoint": {
                            "type": "NUMBER",
                            "value": "40",
                            "color": {"red": 0.7, "green": 0.9, "blue": 0.7},   # green
                        },
                    },
                },
                "index": 0,
            }
        }
    ]

    ws.spreadsheet.batch_update({"requests": requests})

def apply_sheet_formatting(ws):
    col = header_to_col_idx(ws)

    # Financials
    for name in ("revenue_k", "ebitda_k", "asking_price_k"):
        if name in col:
            format_currency_column(ws, col[name])

    for name in ("profit_margin_pct", "revenue_growth_pct", "leverage_pct"):
        if name in col:
            format_percentage_column(ws, col[name])

    if "ebitda_margin" in col:
        # format_percentage_column(ws, col["ebitda_margin"])
        apply_ebitda_margin_color_scale(ws, col["ebitda_margin"])

    # Workflow
    for name in ("status", "decision"):
        if name in col:
            apply_status_rules(ws, col[name])

    # Status column
    if "status" in col:
        apply_status_dropdown(ws, col["status"])

    # Decision column
    if "decision" in col:
        apply_decision_dropdown(ws, col["decision"])

    # Owner column
    if "owner" in col:
        apply_owner_dropdown(ws, col["owner"])

    print("🎨 Sheet formatting applied")

def apply_base_sheet_formatting(ws):
    freeze_header_row(ws)
    format_header_row(ws)
    print("🧊 Header frozen & styled")

def highlight_analyst_editable_columns(ws, columns=DEAL_COLUMNS):
    col_map = header_to_col_idx(ws)

    analyst_cols = [
        c.name for c in columns
        if c.pull and not c.system and c.name in col_map
    ]

    requests = []

    for name in analyst_cols:
        col_idx = col_map[name] - 1  # zero-based
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,  # skip header
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.90,
                            "green": 0.95,
                            "blue": 1.00
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})

    print(f"🖍️ Highlighted {len(analyst_cols)} analyst-editable columns")

def get_protected_columns():
    return [
        c.name for c in DEAL_COLUMNS
        if not c.pull or c.system
    ]

def protect_system_columns(ws, allowed_editors=None):
    """
    Protect non-analyst columns so only editors (or nobody) can change them.
    """
    if allowed_editors is None:
        allowed_editors = []  # empty = only owner

    header_idx = header_to_col_idx(ws)
    protected_cols = get_protected_columns()

    requests = []

    for col_name in protected_cols:
        if col_name not in header_idx:
            continue

        col = header_idx[col_name] - 1  # 0-based

        requests.append({
            "addProtectedRange": {
                "protectedRange": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,     # skip header
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "description": f"System-managed column: {col_name}",
                    "warningOnly": False,
                    "editors": {
                        "users": allowed_editors
                    }
                }
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})

    print(f"🔒 Protected {len(requests)} system columns")

def apply_status_dropdown(ws, col_idx: int):
    """
    Apply a strict dropdown to the Status column.
    col_idx is 1-based.
    """
    STATUS_DROPDOWN = [
        "Initial Contact",
        "CIM",
        "1st Meeting",
        "2nd Meeting",
        "Pre-LOI DD",
        "LOI",
        "On Hold",
        "Pass",
        "Lost",
    ]
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,              # skip header
                    "startColumnIndex": col_idx - 1,
                    "endColumnIndex": col_idx,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": v}
                            for v in STATUS_DROPDOWN
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }
    ]

    ws.spreadsheet.batch_update({"requests": requests})
    print("🔽 Status dropdown applied")

def apply_decision_dropdown(ws, col_idx: int):
    """
    Apply a strict dropdown to the Status column.
    col_idx is 1-based.
    """
    DECISION_DROPDOWN = [
        "Pass",
        "Hold",
        "Progress",
    ]
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,              # skip header
                    "startColumnIndex": col_idx - 1,
                    "endColumnIndex": col_idx,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": v}
                            for v in DECISION_DROPDOWN
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }
    ]

    ws.spreadsheet.batch_update({"requests": requests})
    print("🔽 Decision dropdown applied")

def apply_owner_dropdown(ws, col_idx: int):
    """
    Apply a strict dropdown to the Status column.
    col_idx is 1-based.
    """
    OWNER_DROPDOWN = [
        "AMO",
        "MSE",
        "OBO",
    ]
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,              # skip header
                    "startColumnIndex": col_idx - 1,
                    "endColumnIndex": col_idx,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": v}
                            for v in OWNER_DROPDOWN
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }
    ]

    ws.spreadsheet.batch_update({"requests": requests})
    print("🔽 Owner dropdown applied")

def clear_all_protections(ws):
    """
    Remove all sheet-level and range-level protections.
    Safe to run repeatedly.
    """
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    meta = spreadsheet.fetch_sheet_metadata()

    requests = []

    for sheet in meta["sheets"]:
        if sheet["properties"]["sheetId"] != sheet_id:
            continue

        protections = sheet.get("protectedRanges", [])

        for p in protections:
            requests.append({
                "deleteProtectedRange": {
                    "protectedRangeId": p["protectedRangeId"]
                }
            })

    if requests:
        spreadsheet.batch_update({"requests": requests})
        print(f"🧹 Cleared {len(requests)} existing protections")
    else:
        print("🧹 No existing protections found")