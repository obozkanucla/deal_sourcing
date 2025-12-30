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
    reset_sheet_state
)
from src.integrations.sheets_sync import ensure_sheet_headers

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"

DB_PATH = Path("db/deals.sqlite")

def main():
    repo = SQLiteRepository(DB_PATH)
    print("USING DB:", repo.db_path.resolve())
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    reset_sheet_state(ws, num_columns=len(DEAL_COLUMNS))

    # Pull analyst edits back
    pull_sheets_to_sqlite(
        repo,
        ws,
        columns=DEAL_COLUMNS
    )

    # ✅ SINGLE SOURCE OF TRUTH
    ensure_sheet_headers(ws, DEAL_COLUMNS)

    # Push new deals only
    push_sqlite_to_sheets(
        repo,
        ws
    )
    apply_sheet_formatting(ws)
    apply_base_sheet_formatting(ws)

    # Backfill Drive folder links
    update_folder_links(repo, ws)

    backfill_system_columns(
        repo,
        ws,
        columns=["title", "industry", "sector", "location"]
    )


if __name__ == "__main__":
    main()