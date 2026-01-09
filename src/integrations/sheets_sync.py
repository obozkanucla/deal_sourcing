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
        "Pass",
        "Initial Contact",
        "CIM",
        "CIM DD",
        "LOI",
        "Lost"
    ],
    "priority": [
        "High",
        "Medium",
        "Low",
    ],
    "decision": [
        "Pass",
        "Park",
        "Progress",
    ],
    "owner": [
        "AMO",
        "MSE",
        "OBO",
    ],
    "pass_reason":[
        "Size",
        "Sector",
        "Fundamentals",
        "Process",
        "Valuation",
        "Geography"
    ]
}
NUMERIC_FIELDS = {
    "revenue_k",
    "ebitda_k",
    "asking_price_k",
}

STATUS_RULES = {
    "status": {
        "Pass": {"red": 0.95, "green": 0.8, "blue": 0.8},
        "Initial Contact": {"red": 0.85, "green": 0.9, "blue": 1.0},
        "CIM": {"red": 0.8, "green": 0.95, "blue": 0.8},
        "CIM DD": {"red": 0.75, "green": 0.9, "blue": 0.75},
        "LOI": {"red": 0.7, "green": 0.9, "blue": 0.7},
        "Lost": {"red": 0.9, "green": 0.6, "blue": 0.6},
    },
    "decision": {
        "Pass": {"red": 0.95, "green": 0.8, "blue": 0.8},
        "Progress": {"red": 0.8, "green": 0.95, "blue": 0.8},
        "Park": {"red": 1.0, "green": 0.9, "blue": 0.6},
    },
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
import time
import random
from gspread.exceptions import APIError

def sheets_write_with_backoff(fn, *, max_retries=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except APIError as e:
            if "Quota exceeded" not in str(e):
                raise
            sleep = 2 ** attempt + random.random()
            print(f"⏳ Sheets quota hit — retrying in {sleep:.1f}s")
            time.sleep(sleep)
    raise RuntimeError("Sheets quota exceeded after retries")


def ensure_sheet_headers(ws, columns):
    expected = [c.name for c in columns]

    # 🔥 FULL RESET: clear sheet completely
    ws.clear()

    # Resize to exactly 1 row and N columns
    ws.resize(rows=2, cols=len(expected))

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
    return len(rows)

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

    updates = []

    for row_idx, row in enumerate(rows, start=2):  # header = row 1
        deal_uid = row[deal_uid_col].strip() if deal_uid_col < len(row) else ""
        current_link = row[folder_col].strip() if folder_col < len(row) else ""

        if not deal_uid or current_link:
            continue

        try:
            source, source_listing_id = deal_uid.split(":", 1)
        except ValueError:
            continue

        deal = repo.fetch_by_source_and_listing(source, source_listing_id)
        if not deal:
            continue

        drive_url = deal.get("drive_folder_url")
        if not drive_url:
            continue

        updates.append({
            "range": f"{ws.title}!{chr(ord('A') + folder_col)}{row_idx}",
            "values": [[f'=HYPERLINK("{drive_url}", "Folder")']]
        })

    if not updates:
        print("✅ No Drive Folder links to update")
        return

    # 🔑 SINGLE write
    ws.batch_update(updates)

    print(f"✅ Updated {len(updates)} Drive Folder links")

def backfill_system_columns(repo, ws, columns, batch_size=100, force=False):
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
            if not force:
                if col_idx < len(row) and row[col_idx].strip():
                    if col not in ("ebitda_margin", "revenue_multiple", "ebitda_multiple"):
                        continue

            val = deal.get(col)
            if val is None:
                continue

            updates.append({
                "range": rowcol_to_a1(row_num, col_idx + 1),
                "values": [[val]],
            })

        # Flush in batches #
        if len(updates) >= batch_size:
            sheets_write_with_backoff(lambda: ws.batch_update(updates))
            updates.clear()
            time.sleep(1)  # stay well under quota

    # Flush remaining updates
    if updates:
        sheets_write_with_backoff(lambda: ws.batch_update(updates))

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


def apply_status_format_rules(ws, col_name: str, col_idx: int):
    if col_name not in STATUS_RULES:
        return

    rules = STATUS_RULES[col_name]
    requests = []

    for value, color in rules.items():
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": ws.id,
                        "startRowIndex": 1,  # skip header
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

def clear_all_conditional_formatting(ws):
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    meta = spreadsheet.fetch_sheet_metadata()
    rules = []

    for sheet in meta["sheets"]:
        if sheet["properties"]["sheetId"] == sheet_id:
            rules = sheet.get("conditionalFormats", [])

    requests = [
        {
            "deleteConditionalFormatRule": {
                "sheetId": sheet_id,
                "index": 0
            }
        }
        for _ in rules
    ]

    if requests:
        spreadsheet.batch_update({"requests": requests})

def reset_sheet_state(ws, num_columns: int):
    sheet_id = ws.id
    spreadsheet = ws.spreadsheet

    # 1️⃣ Unfreeze
    spreadsheet.batch_update({
        "requests": [{
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
        }]
    })

    time.sleep(0.5)  # allow Sheets to settle

    # 2️⃣ Clear values
    ws.clear()

    # 3️⃣ Resize safely
    ws.resize(rows=2, cols=num_columns)

    print("🧼 Sheet fully reset (safe resize)")

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


    dropdown_cols = ["status", "decision", "owner"]
    apply_dropdown_validations(ws)
    for name in dropdown_cols:
        apply_status_format_rules(ws, name, col[name])


    print("🎨 Sheet formatting applied")

def apply_pass_reason_required_formatting(ws):
    """
    Highlight rows where:
    - status == 'pass'
    - pass_reason is blank

    Uses column letters (API-safe), resolved dynamically from headers.
    """
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    # Read header row
    headers = ws.row_values(1)
    header_map = {h: idx + 1 for idx, h in enumerate(headers)}

    if "status" not in header_map or "pass_reason" not in header_map:
        print("⚠️ status or pass_reason column missing — skipping formatting")
        return

    def col_letter(n: int) -> str:
        """1 -> A, 27 -> AA"""
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    status_col = col_letter(header_map["status"])
    reason_col = col_letter(header_map["pass_reason"])

    formula = (
        f'=AND(${status_col}2="pass", ISBLANK(${reason_col}2))'
    )

    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {
                            "sheetId": sheet_id,
                            "startRowIndex": 1  # skip header
                        }
                    ],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [
                                {"userEnteredValue": formula}
                            ]
                        },
                        "format": {
                            "backgroundColor": {
                                "red": 1.0,
                                "green": 0.85,
                                "blue": 0.85
                            },
                            "textFormat": {
                                "bold": True
                            }
                        }
                    }
                },
                "index": 0
            }
        }
    ]

    spreadsheet.batch_update({"requests": requests})
    print("🚦 Pass reason requirement formatting applied")

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

def apply_dropdown(ws, col_name: str, col_idx: int, values: list[str]):
    """
    Apply a strict dropdown validation to a column.
    col_idx is 1-based.
    """
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
                            {"userEnteredValue": v} for v in values
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }
    ]

    ws.spreadsheet.batch_update({"requests": requests})
    print(f"🔽 {col_name} dropdown applied")

def apply_dropdown_validations(ws):
    col_map = header_to_col_idx(ws)

    for col_name, values in DROPDOWNS.items():
        if col_name not in col_map:
            continue
        apply_dropdown(
            ws=ws,
            col_name=col_name,
            col_idx=col_map[col_name],
            values=values,
        )

def clear_all_protections(ws):
    """
    HARD RESET:
    Removes all protected ranges and sheet protections
    from the given worksheet.
    Safe, idempotent, production-safe.
    """

    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    meta = spreadsheet.fetch_sheet_metadata()
    requests = []

    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") != sheet_id:
            continue

        # 1️⃣ Remove protected ranges
        for pr in sheet.get("protectedRanges", []):
            requests.append({
                "deleteProtectedRange": {
                    "protectedRangeId": pr["protectedRangeId"]
                }
            })

        # 2️⃣ Remove sheet protection (if present)
        if "protectedSheet" in sheet:
            requests.append({
                "updateProtectedRange": {
                    "protectedRange": {
                        "protectedRangeId": sheet["protectedSheet"]["protectedRangeId"],
                        "protectedSheet": None
                    },
                    "fields": "protectedSheet"
                }
            })

    if requests:
        spreadsheet.batch_update({"requests": requests})
        print(f"🧹 Cleared {len(requests)} protections")
    else:
        print("🧹 No protections found")

def clear_sheet_filter(ws):
    """
    Clears filters and protections.
    Safe to run repeatedly.
    """
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    meta = spreadsheet.fetch_sheet_metadata()
    requests = []

    for sheet in meta["sheets"]:
        if sheet["properties"]["sheetId"] != sheet_id:
            continue

        # 1️⃣ Clear basic filter if present
        if "basicFilter" in sheet:
            requests.append({
                "clearBasicFilter": {
                    "sheetId": sheet_id
                }
            })

    if requests:
        spreadsheet.batch_update({"requests": requests})
        print(f"🧹 Cleared {len(requests)} sheet  filters")
    else:
        print("🧹 Sheet already clean")

def apply_filter_to_used_range(ws, num_rows, num_cols):
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    spreadsheet.batch_update({
        "requests": [{
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": num_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    }
                }
            }
        }]
    })

    print("🔎 Filter reapplied to full data range")

def _get_col_index(columns, name):
    for i, c in enumerate(columns):
        if c.name == name:
            return i
    return None

def apply_left_alignment(ws, column_names):
    spreadsheet = ws.spreadsheet
    sheet_id = ws.id

    header_idx = header_to_col_idx(ws)  # ← source of truth

    requests = []

    for col_name in column_names:
        if col_name not in header_idx:
            continue

        col_idx = header_idx[col_name] - 1  # 0-based

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT"
                    }
                },
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        })

    if requests:
        spreadsheet.batch_update({"requests": requests})
        print(f"↔️ Left-aligned columns: {', '.join(column_names)}")