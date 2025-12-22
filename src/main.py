from datetime import date
from pathlib import Path

from brokers.business_buyers import BusinessBuyersClient
from utils.rate_limit import DailyClickBudget, BudgetExhausted
from utils.hashing import hash_text

from extraction.html_cleaner import extract_clean_text
from extraction.pdf_snapshot import save_pdf
from decisioning.llm_classifier import classify_listing
from decisioning.rules import apply_hard_rules
from src.persistence.repository import SQLiteRepository

from config.settings import (
    BB_USERNAME,
    BB_PASSWORD,
    DAILY_DETAIL_PAGE_BUDGET,
    PDF_DIR,
)

DB_PATH = Path("db/deals.sqlite")


def main():
    repo = SQLiteRepository(DB_PATH)

    today = date.today()
    already_used = repo.get_clicks_used("BusinessBuyers", today)

    budget = DailyClickBudget(DAILY_DETAIL_PAGE_BUDGET - already_used)

    client = BusinessBuyersClient(
        BB_USERNAME,
        BB_PASSWORD,
        budget
    )

    client.login()
    client.fetch_index_listings()
    listings = repo.get_pending_index_records("BusinessBuyers")

    for listing in listings:
        source = "BusinessBuyers"
        listing_id = listing["source_listing_id"]

        if repo.deal_exists(source, listing_id):
            continue  # already processed (or pending change detection)

        try:
            html = client.fetch_listing_detail(listing)
            repo.increment_clicks(source, today)

            pdf_path = save_pdf(
                client.page,
                listing_id,
                PDF_DIR
            )

            cleaned = extract_clean_text(html)
            content_hash = hash_text(cleaned)

            llm_result = classify_listing(cleaned)
            final_decision = apply_hard_rules(llm_result)

            repo.upsert_deal(
                source=source,
                source_listing_id=listing_id,
                source_url=listing["source_url"],
                content_hash=content_hash,
                decision=final_decision,
                decision_confidence=llm_result["classification"]["decision_confidence"],
                reasons="; ".join(llm_result["classification"]["reasons"]),
                extracted_json=str(llm_result),
                pdf_path=pdf_path,
            )

            print(listing_id, final_decision)

        except BudgetExhausted:
            print("Daily BB click budget exhausted.")
            break


if __name__ == "__main__":
    main()