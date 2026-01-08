from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))

with repo.get_conn() as conn:
    cur = conn.execute(
        "ALTER TABLE deals ADD COLUMN turnover_range_raw TEXT;"
    )
    conn.commit()
    conn.close()
print(f"Added turnover_range_raw column.")