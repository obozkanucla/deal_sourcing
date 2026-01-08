from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))

with repo.get_conn() as conn:
    cur = conn.execute(
        "ALTER TABLE deals ADD COLUMN sector_raw TEXT;"
        "ALTER TABLE deals ADD COLUMN location_raw TEXT;"
    )
    conn.commit()
print(f"Added turnover_range_raw column.")