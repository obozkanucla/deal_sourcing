from pathlib import Path
from src.persistence.repository import SQLiteRepository
from src.brokers.dealopportunities_client import DealOpportunitiesClient

DRY_RUN = False


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    client = DealOpportunitiesClient()
    client.start()          # 🔑 REQUIRED

    try:
        rows = client.fetch_index(max_pages=100)
    finally:
        client.stop()       # 🔑 ALWAYS clean up

    inserted = 0
    refreshed = 0

    for r in rows:
        exists = repo.deal_exists(
            r["source"],
            r["source_listing_id"],
        )

        if not DRY_RUN:
            repo.upsert_index_only(
                source=r["source"],
                source_listing_id=r["source_listing_id"],
                source_url=r["source_url"],
                sector_raw=r.get("sectors_multi"),   # ✅ FIXED
            )

        if exists:
            refreshed += 1
        else:
            inserted += 1

    print(
        f"✅ DealOpportunities import complete — "
        f"inserted={inserted}, refreshed={refreshed}"
    )


if __name__ == "__main__":
    main()