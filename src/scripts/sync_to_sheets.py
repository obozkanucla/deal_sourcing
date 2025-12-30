"""
Sheets sync contract:
- SQLite is source of truth for new rows
- Google Sheets is source of truth for analyst-edited fields
- Script is safe to run repeatedly (idempotent)
"""


import time
from pathlib import Path

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.integrations.sheets_sync import (
    push_sqlite_to_sheets,
    pull_sheets_to_sqlite,
    update_folder_links,
    backfill_system_columns
)

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "DealsV2"

DB_PATH = Path("db/deals.sqlite")

ALLOWED_COLUMNS = [
    "deal_uid",
    "source",
    "source_listing_id",
    "source_url",
    "title",
    "industry",
    "sector",
    "location",
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
    "drive_folder_url"
]

def main():
    repo = SQLiteRepository(DB_PATH)
    print("USING DB:", repo.db_path.resolve())
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # Push new deals only
    push_sqlite_to_sheets(
        repo,
        ws,
        allowed_columns=ALLOWED_COLUMNS,
    )

    # Backfill Drive folder links
    update_folder_links(repo, ws)

    # Pull analyst edits back
    pull_sheets_to_sqlite(
        repo,
        ws,
        allowed_columns=ALLOWED_COLUMNS,
    )

    backfill_system_columns(
        repo,
        ws,
        columns=["title", "industry", "sector", "location"]
    )

if __name__ == "__main__":
    main()