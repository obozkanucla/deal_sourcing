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
FULL_REBUILD = False
PHASE = os.getenv("SHEETS_PHASE", "DATA").upper()
assert PHASE in {"DATA", "FORMAT"}, f"Invalid SHEETS_PHASE: {PHASE}"

PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local")  # local | github
# SHEET_MODE = os.getenv("SHEET_MODE", "test") # prod | test

if PIPELINE_ENV == "prod":
    SPREADSHEET_ID = SPREADSHEET_ID_Production
else:
    SPREADSHEET_ID = SPREADSHEET_ID_Staging

DB_PATH = Path("db/deals.sqlite")

import time
from requests.exceptions import ConnectionError

def safe_get_all_values(ws, retries=5):
    for i in range(retries):
        try:
            return ws.get_all_values()
        except ConnectionError as e:
            if i == retries - 1:
                raise
            sleep = 2 ** i
            print(f"⚠️ Sheets read failed, retrying in {sleep}s")
            time.sleep(sleep)

def sheets_sleep(base=0.3, jitter=0.4):
    time.sleep(base + random.random() * jitter)

def main():
    print("📄 Google Sheet:")
    print(PIPELINE_ENV, "| PHASE:", PHASE)

    repo = SQLiteRepository(DB_PATH)
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 1️⃣ ALWAYS pull analyst edits first
    pull_sheets_to_sqlite(repo, ws, columns=DEAL_COLUMNS)
    repo.recompute_effective_fields()
    recalculate_financial_metrics()

    # ======================================================
    # 🟦 DATA PHASE — writes only
    # ======================================================
    if PHASE == "DATA":

        if FULL_REBUILD:
            print("🧨 FULL REBUILD MODE (DATA PHASE)")
            reset_sheet_state(ws, num_columns=len(DEAL_COLUMNS))
            ensure_sheet_headers(ws, DEAL_COLUMNS)
            assert_schema_alignment(repo, ws)

            push_sqlite_to_sheets(repo, ws)
            print("✅ DATA PHASE COMPLETE (FULL REBUILD)")
            return

        # incremental
        ensure_sheet_headers(ws, DEAL_COLUMNS)
        assert_schema_alignment(repo, ws)

        push_sqlite_to_sheets(repo, ws)

        print("✅ DATA PHASE COMPLETE (INCREMENTAL)")
        return
    # ======================================================
    # 🟩 FORMAT PHASE — reads + formatting only
    # ======================================================
    if PHASE == "FORMAT":

        values = safe_get_all_values(ws)
        num_rows = len(values)
        num_cols = len(DEAL_COLUMNS)

        headers = ws.row_values(1)
        expected = [c.name for c in DEAL_COLUMNS]
        assert headers == expected

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

        # update_folder_links(repo, ws)
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

        hide_columns(ws,["canonical_external_id", "broker_name", "broker_listing_url"])

        shrink_columns_by_name(ws, ["deal_uid", "source_listing_id"], width_px=2)
        shrink_columns_by_name(ws, ["revenue_k", "ebitda_k", "asking_price_k"], width_px=2)

        highlight_analyst_editable_columns(ws)
        protect_system_columns(
            ws,
            ["burak@sab.partners", "serdar@sab.partners", "adrien@sab.partners"]
        )

        print("✅ FORMAT PHASE COMPLETE")

if __name__ == "__main__":
    main()