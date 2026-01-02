# src/scripts/import_businesses4sale.py

from pathlib import Path
from src.brokers.businesses4sale_client import BusinessesForSaleClient
from src.persistence.repository import SQLiteRepository

DRY_RUN = False  # 🔒 keep True until confident


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    print("🚀 import_businesses4sale.py started")

    client = BusinessesForSaleClient(
        headless=False,
        slow_mo_ms=0,
        max_pages=10,  # adjust later
    )

    records = client.fetch_index()
    print(f"📦 Records returned: {len(records)}")

    for rec in records[:5]:
        print("🧪", rec)

    if DRY_RUN:
        print("\n🧪 DRY RUN — no database writes performed")
        return

    # later:
    # repo.insert_raw_deal(...)

    inserted = 0
    for rec in records:
        repo.upsert_index_only(
            source=rec["source"],
            source_listing_id=rec["source_listing_id"],
            source_url=rec["source_url"],
        )
        inserted += 1

    print(f"✅ BusinessesForSale index imported: {inserted}")

if __name__ == "__main__":
    main()