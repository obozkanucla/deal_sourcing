#enrich_financials_from_description.py

from pathlib import Path
from src.persistence.repository import SQLiteRepository
from src.enrichment.financial_extractor import extract_financial_metrics

DRY_RUN = False


def is_plausible_k(value: int) -> bool:
    # £10k – £10bn (values are in thousands)
    return 10 <= value <= 10_000_000


def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    deals = repo.fetch_deals_with_descriptions()

    updated = 0
    print("🔍 TOTAL DEALS FETCHED:", len(deals))

    for deal in deals:
        print(
            f"➡️ DEAL {deal['id']} | source={deal['source']} | "
            f"has_desc={bool(deal.get('description'))} | "
            f"rev={deal.get('revenue_k')} ebitda={deal.get('ebitda_k')}"
        )

        description = deal.get("description")
        if not description:
            continue

        extracted = extract_financial_metrics(description)
        print("🧪 EXTRACTED RAW:", extracted)

        if not extracted:
            continue

        updates = {}

        for field in ("revenue_k", "ebitda_k", "asking_price_k"):
            data = extracted.get(field)
            if not data:
                continue

            # 🔒 Rules:
            # - only fill missing values
            # - only numeric scalars
            # - only plausible magnitudes

            value = data.get("value")
            if (
                    value is not None
                    and deal.get(field) is None
                    and is_plausible_k(value)
            ):
                updates[field] = value


            print(
                f"🔎 FIELD={field} VALUE={value} "
                f"PLAUSIBLE={is_plausible_k(value) if value else False} "
                f"EXISTING={deal.get(field)}"
            )

        if not updates:
            continue

        print("✍️ UPDATING:", updates)

        if DRY_RUN:
            print("—" * 80)
            print(f"DEAL ID: {deal['id']} ({deal['source']})")
            print(description[:300], "...")
            print("UPDATES:", updates)
        else:
            repo.update_deal_fields(
                source=deal["source"],
                source_listing_id=deal["source_listing_id"],
                updates=updates,
            )
            updated += 1

    print(f"💰 Financial enrichment complete — updated {updated} deals")

if __name__ == "__main__":
    main()