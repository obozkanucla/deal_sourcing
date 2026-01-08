from pathlib import Path
from src.persistence.repository import SQLiteRepository
from src.scripts import import_legacy_deals
DB_PATH = Path("db/deals.sqlite")

def reset_legacy():
    repo = SQLiteRepository(DB_PATH)

    with repo.get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM deals WHERE source = 'LegacySheet'"
        )
        conn.commit()

    print(f"🧹 Deleted {cur.rowcount} legacy deals")

if __name__ == "__main__":
    reset_legacy()
    import_legacy_deals.main()