from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))

with repo.get_conn() as conn:
    conn.execute("ALTER TABLE deals ADD COLUMN sector_raw TEXT;")
    conn.execute("ALTER TABLE deals ADD COLUMN location_raw TEXT;")
    conn.commit()

print("Added sector_raw and location_raw columns.")