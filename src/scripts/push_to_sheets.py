import time
from pathlib import Path

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.integrations.sheets_sync import (
    push_sqlite_to_sheets,
    pull_sheets_to_sqlite,
    update_folder_links,
)

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"

DB_PATH = Path("db/deals.sqlite")

def main():
    repo = SQLiteRepository(DB_PATH)

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # Push new deals only
    push_sqlite_to_sheets(repo, ws)

    # Backfill Drive folder links
    update_folder_links(repo, ws)

    # Pull analyst edits back
    pull_sheets_to_sqlite(repo, ws)

if __name__ == "__main__":
    main()