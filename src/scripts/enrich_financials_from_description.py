from pathlib import Path
from src.persistence.repository import SQLiteRepository
from src.enrichment.financial_extractor import extract_financial_metrics
DRY_RUN = True

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_deals_with_descriptions()
    updated = 0

    for deal in deals:
        if not deal["description"]:
            continue

        extracted = extract_financial_metrics(deal["description"])

        updates = {}
        for field in ("revenue_k", "ebitda_k", "asking_price_k"):
            value = extracted.get(field)
            if deal.get(field) is None and value is not None:
                updates[field] = extracted[field]

        if updates:
            if DRY_RUN:
                print("—" * 80)
                print(f"DEAL: {deal['id']}")
                print(deal["description"][:300], "...")
                print("EXTRACTED:", updates)
            else:
                repo.update_deal_fields(deal["id"], updates)
                updated += 1

    print(f"💰 Financial enrichment complete — updated {updated} deals")

if __name__ == "__main__":
    main()