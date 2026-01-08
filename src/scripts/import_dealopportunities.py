from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.brokers.dealopportunities_client import DealOpportunitiesClient

DRY_RUN = False


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    now = datetime.utcnow().isoformat(timespec="seconds")

    client = DealOpportunitiesClient()
    client.start()          # 🔑 REQUIRED

    try:
        rows = client.fetch_index(max_pages=100)
    finally:
        client.stop()       # 🔑 ALWAYS clean up

    inserted = 0
    refreshed = 0

    for r in rows:
        source = r["source"]
        source_listing_id = r["source_listing_id"]

        exists = repo.deal_exists(source, source_listing_id)

        if DRY_RUN:
            print(
                source_listing_id,
                r.get("location"),
                r.get("turnover_range"),
            )
            continue

        # -------------------------------------------------
        # INDEX-LEVEL UPSERT (RAW, LOSSLESS)
        # -------------------------------------------------
        repo.upsert_index_only(
            source=source,
            source_listing_id=source_listing_id,
            source_url=r["source_url"],

            # raw broker truth
            sector_raw=r.get("sectors_multi"),
            location_raw=r.get("location"),
            turnover_range_raw=r.get("turnover_range"),

            # lifecycle
            first_seen=None if exists else now,
            last_seen=now,
            last_updated=now,
            last_updated_source="AUTO",
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