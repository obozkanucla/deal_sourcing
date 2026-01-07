"""
Sheets sync contract:
- SQLite is source of truth for new rows
- Google Sheets is source of truth for analyst-edited fields
- Script is safe to run repeatedly (idempotent)
"""


import time
from pathlib import Path

from src.domain.deal_columns import DEAL_COLUMNS
from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
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
    clear_all_protections
)
from src.integrations.sheets_sync import ensure_sheet_headers

SPREADSHEET_ID_Production = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"
SPREADSHEET_ID_Staging = "1Iioxt688xxw9fVbiixAMGrycwl22GqSh91p4EYsEAH0"
DROPDOWNS = {
    "status": [
        "Pass",
        "Initial Contact",
        "CIM",
        "CIM DD",
        "1st Meeting (online)",
        "2nd Meeting (in person)",
         "Pre-LOI DD",
        "LOI",
        "On-Hold (UOffer)",
        "Lost"],
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

import os

PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local")  # local | github
SHEET_MODE = os.getenv("SHEET_MODE", "test")       # prod | test

if SHEET_MODE == "prod":
    SPREADSHEET_ID = SPREADSHEET_ID_Production
else:
    SPREADSHEET_ID = SPREADSHEET_ID_Staging

DB_PATH = Path("db/deals.sqlite")

def main():
    print("📄 Google Sheet:")
    print(f"   Spreadsheet ID: {SPREADSHEET_ID}")
    print(f"   Worksheet: {WORKSHEET_NAME}")
    print(f"   URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    repo = SQLiteRepository(DB_PATH)
    print("USING DB:", repo.db_path.resolve())

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 1️⃣ PULL analyst edits FIRST
    pull_sheets_to_sqlite(
        repo,
        ws,
        columns=DEAL_COLUMNS
    )

    # 2️⃣ Clean sheet
    reset_sheet_state(ws, num_columns=len(DEAL_COLUMNS))

    # Clear all protections
    clear_all_protections(ws)

    # 3️⃣ Ensure headers (safe, non-destructive)
    ensure_sheet_headers(ws, DEAL_COLUMNS)

    # 4️⃣ Push new deals only
    push_sqlite_to_sheets(repo, ws)

    apply_sheet_formatting(ws)
    apply_base_sheet_formatting(ws)

    # 5️⃣ Enrichment / backfills
    update_folder_links(repo, ws)

    backfill_system_columns(
        repo,
        ws,
        columns=[
            "title",
            "industry",
            "sector",
            "location",
            "profit_margin_pct",
            "revenue_growth_pct",
            "leverage_pct",
        ]
    )

    highlight_analyst_editable_columns(ws)
    protect_system_columns(ws,["burak@sab.partners",
                               "serdar@sab.partners",
                               "adrien@sab.partners"])

if __name__ == "__main__":
    main()