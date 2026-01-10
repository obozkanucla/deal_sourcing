"""
Sheets sync contract:
- SQLite is source of truth for new rows
- Google Sheets is source of truth for analyst-edited fields
- Script is safe to run repeatedly (idempotent)
"""

from pathlib import Path
import os
import time
import random

from src.domain.deal_columns import DEAL_COLUMNS
from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.scripts.recalculate_financial_metrics import main as recalculate_financial_metrics
from src.integrations.sheets_sync import (
    push_sqlite_to_sheets,
    pull_sheets_to_sqlite,
    update_folder_links,
    backfill_system_columns,
    apply_sheet_formatting,
    apply_base_sheet_formatting,
    reset_sheet_state,
    highlight_analyst_editable_columns,
    protect_system_columns,
    clear_all_protections,
    apply_dropdown_validations,
    clear_sheet_filter,
    apply_filter_to_used_range,
    apply_pass_reason_required_formatting,
    apply_left_alignment,
    hide_columns,
    unhide_all_columns,
    assert_schema_alignment,
    shrink_columns_by_name
)
from src.integrations.sheets_sync import ensure_sheet_headers

SPREADSHEET_ID_Production = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"
SPREADSHEET_ID_Staging = "1Iioxt688xxw9fVbiixAMGrycwl22GqSh91p4EYsEAH0"
FULL_REBUILD = True

PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local")  # local | github
# SHEET_MODE = os.getenv("SHEET_MODE", "test")       # prod | test

if PIPELINE_ENV == "prod":
    SPREADSHEET_ID = SPREADSHEET_ID_Production
else:
    SPREADSHEET_ID = SPREADSHEET_ID_Staging

DB_PATH = Path("db/deals.sqlite")

def sheets_sleep(base=0.3, jitter=0.4):
    time.sleep(base + random.random() * jitter)

def main():
    print("📄 Google Sheet:")
    print(PIPELINE_ENV)
    print(f"   Spreadsheet ID: {SPREADSHEET_ID}")
    print(f"   Worksheet: {WORKSHEET_NAME}")
    print(f"   URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    repo = SQLiteRepository(DB_PATH)
    print("USING DB:", repo.db_path.resolve())

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # unhide_all_columns(ws)

    # 1️⃣ PULL analyst edits FIRST
    pull_sheets_to_sqlite(repo, ws, columns=DEAL_COLUMNS)
    sheets_sleep()

    repo.recompute_effective_fields()
    recalculate_financial_metrics()
    sheets_sleep()

    # 2️⃣ Optional destructive rebuild
    reset_sheet_state(ws, num_columns=len(DEAL_COLUMNS))
    sheets_sleep()

    # 3️⃣ ALWAYS ensure headers (safe)
    ensure_sheet_headers(ws, DEAL_COLUMNS)
    assert_schema_alignment(repo, ws)
    sheets_sleep()

    # 🔒 HARD SAFETY CHECK — FAIL FAST IF ORDER DRIFTS
    headers = ws.row_values(1)
    expected = [c.name for c in DEAL_COLUMNS]
    assert headers == expected, (
        "❌ Sheet headers do not match DEAL_COLUMNS order.\n"
        f"Expected: {expected}\n"
        f"Found:    {headers}"
    )

    # 4️⃣ Push SQLite → Sheets
    push_sqlite_to_sheets(repo, ws)
    sheets_sleep()

    # ✅ Always derive dimensions from sheet, never from rows_written
    values = ws.get_all_values()
    num_rows = len(values)
    num_cols = len(DEAL_COLUMNS)

    # 5️⃣ Formatting (gate heavy ops)
    if FULL_REBUILD or num_rows > 0:
        apply_dropdown_validations(ws)
        sheets_sleep()

        apply_sheet_formatting(ws)
        sheets_sleep()

        apply_base_sheet_formatting(ws)
        sheets_sleep()

        clear_sheet_filter(ws)
        sheets_sleep()

        clear_all_protections(ws)
        sheets_sleep()

        apply_filter_to_used_range(ws, num_rows, num_cols)
        sheets_sleep()

        apply_pass_reason_required_formatting(ws)
        sheets_sleep()

        apply_left_alignment(ws)
        sheets_sleep()

    # 6️⃣ Backfills (safe, idempotent)
    update_folder_links(repo, ws)
    sheets_sleep()

    backfill_system_columns(
        repo,
        ws,
        columns=[
            "title",
            "industry",
            "sector",
            "location",
            "ebitda_margin",
            "revenue_growth_pct",
            "leverage_pct",
        ]
    )
    # A = deal_uid, C = source_listing_id
    shrink_columns_by_name(
        ws,
        ["deal_uid", "source_listing_id"],
        width_px=2
    )
    shrink_columns_by_name(
        ws,
        ["revenue_k", "ebitda_k", "asking_price_k"],
        width_px=2
    )

    highlight_analyst_editable_columns(ws)
    sheets_sleep()

    protect_system_columns(
        ws,
        ["burak@sab.partners", "serdar@sab.partners", "adrien@sab.partners"]
    )
    sheets_sleep()

if __name__ == "__main__":
    main()