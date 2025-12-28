from pathlib import Path
from src.brokers.axispartnership_client import AxisPartnershipClient
from src.persistence.repository import SQLiteRepository


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    client = AxisPartnershipClient()
    client.start()

    try:
        rows = client.fetch_index()
    finally:
        client.stop()

    inserted = 0
    refreshed = 0

    for row in rows:
        is_new = repo.upsert_index_only(
            source=row["source"],
            source_listing_id=row["source_listing_id"],
            source_url=row["source_url"],
        )

        if is_new:
            inserted += 1
        else:
            refreshed += 1

    print(f"✅ Axis import complete — inserted={inserted}, refreshed={refreshed}")


if __name__ == "__main__":
    main()