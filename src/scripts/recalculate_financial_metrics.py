# src/scripts/recalculate_financial_metrics.py

from pathlib import Path
from src.persistence.repository import SQLiteRepository

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    repo.recalculate_financial_metrics()
    print("✅ Financial metrics recalculated")

if __name__ == "__main__":
    main()