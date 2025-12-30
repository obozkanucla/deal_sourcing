from src.persistence.repository import SQLiteRepository
from pathlib import Path

DB_PATH = Path("db/deals.sqlite")

repo = SQLiteRepository(DB_PATH)

deals = repo.fetch_all_deals()
print(deals[0].keys())