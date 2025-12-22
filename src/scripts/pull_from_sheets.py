from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.integrations.sheets_sync import pull_sheets_to_sqlite
from pathlib import Path

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"

DB_PATH = Path("db/deals.sqlite")  # ← SAME DB AS SCRAPER

def main():
    repo = SQLiteRepository(DB_PATH)
    gc = get_gspread_client()

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    pull_sheets_to_sqlite(repo, ws)

if __name__ == "__main__":
    main()