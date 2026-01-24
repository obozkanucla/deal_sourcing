from pathlib import Path
from datetime import datetime

from src.persistence.repository import SQLiteRepository
from src.brokers.dealopportunities_client import DealOpportunitiesClient

# ----------------------------------
# DRY RUN CONFIG
# ----------------------------------
DRY_RUN = False
DRY_RUN_PAGES = 2
DRY_RUN_PREVIEW_LIMIT = 50  # safety cap for printing


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    now = datetime.today().isoformat()

    client = DealOpportunitiesClient()
    client.start()          # 🔑 REQUIRED

    try:
        rows = client.fetch_index(
            max_pages=DRY_RUN_PAGES if DRY_RUN else 100
        )
    finally:
        client.stop()       # 🔑 ALWAYS clean up

    inserted = 0
    refreshed = 0
    preview = []

    for r in rows:
        source = r["source"]
        source_listing_id = r["source_listing_id"]

        exists = repo.deal_exists(source, source_listing_id)

        if DRY_RUN:
            preview.append({
                "source_listing_id": source_listing_id,
                "location_raw": r.get("location"),
                "turnover_range_raw": r.get("turnover_range"),
                "sector_raw": r.get("sectors_multi"),
                "exists_in_db": exists,
            })
            continue

        # -------------------------------------------------
        # INDEX-LEVEL UPSERT (RAW, LOSSLESS)
        # -------------------------------------------------
        repo.upsert_index_only(
            source=source,
            source_listing_id=source_listing_id,
            source_url=r["source_url"],
            sector_raw=r.get("sectors_multi"),
            first_seen=None if exists else now,
            last_seen=now,
            last_updated=now,
            last_updated_source="AUTO",
        )

        repo.enrich_do_raw_fields(
            source=source,
            source_listing_id=source_listing_id,
            location_raw=r.get("location"),
            turnover_range_raw=r.get("turnover_range"),
        )

        refreshed += int(exists)
        inserted += int(not exists)

    # ----------------------------------
    # DRY RUN OUTPUT
    # ----------------------------------
    if DRY_RUN:
        print(f"\n🧪 DRY RUN — previewing {min(len(preview), DRY_RUN_PREVIEW_LIMIT)} rows\n")

        for row in preview[:DRY_RUN_PREVIEW_LIMIT]:
            print(
                row["source_listing_id"],
                "| location:", row["location_raw"],
                "| turnover:", row["turnover_range_raw"],
                "| sector:", row["sector_raw"],
                "| exists:", row["exists_in_db"],
            )

        print(f"\n🧪 DRY RUN complete — rows_fetched={len(preview)}")
        return

    print(
        f"✅ DealOpportunities import complete — "
        f"inserted={inserted}, refreshed={refreshed}"
    )

if __name__ == "__main__":
    main()