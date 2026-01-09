from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))

with repo.get_conn() as conn:
    cur = conn.execute(
        "DELETE FROM deals WHERE source = 'Dmitry'"
    )
    conn.commit()

    cur = conn.execute(
        "ALTER TABLE deals ADD COLUMN description_hash TEXT"
    )
    conn.commit()

print(f"Deleted {cur.rowcount} Dmitry deals")