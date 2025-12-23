from pathlib import Path

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.integrations.sheets_sync import push_sqlite_to_sheets, update_folder_links

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 1️⃣ Append new deals (unchanged)
    push_sqlite_to_sheets(repo, ws)

    # 2️⃣ Update folder links for existing deals
    update_folder_links(repo, ws)

if __name__ == "__main__":
    main()