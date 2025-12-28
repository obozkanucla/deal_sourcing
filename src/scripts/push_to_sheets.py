from pathlib import Path

from src.persistence.repository import SQLiteRepository
from src.integrations.google_sheets import get_gspread_client
from src.integrations.sheets_sync import push_sqlite_to_sheets, update_folder_links

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"
WORKSHEET_NAME = "Deals"

# 🔒 Explicit ownership
SYSTEM_COLUMNS = {
    "source",
    "source_listing_id",
    "source_url",
    "title",
    "description",
    "location",
    "sector_raw",
    "industry_raw",
    "revenue_k",
    "ebitda_k",
    "asking_price_k",
    "pdf_path",
    "pdf_drive_url",
    "detail_fetched_at",
    "first_seen",
    "last_seen",
    "last_updated",
}

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 1️⃣ Push SYSTEM columns only
    push_sqlite_to_sheets(
        repo,
        ws,
        allowed_columns=SYSTEM_COLUMNS,
        key_columns=("source", "source_listing_id"),
    )

    # 2️⃣ Update folder links (still system-owned)
    update_folder_links(repo, ws)

if __name__ == "__main__":
    main()