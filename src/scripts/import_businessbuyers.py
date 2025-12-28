from src.brokers.businessbuyers_client import BusinessBuyersClient
from src.config import BB_USERNAME, BB_PASSWORD

def main():
    bb = BusinessBuyersClient(
        username=BB_USERNAME,
        password=BB_PASSWORD,
        click_budget=None,
    )

    bb.login()
    bb.fetch_index_listings()

    print("✅ BusinessBuyers index run complete")

if __name__ == "__main__":
    main()