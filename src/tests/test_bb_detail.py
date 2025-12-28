# src/scripts/test_bb_detail.py

from src._to_delete.businessbuyers_detailxx import scrape_bb_detail_from_page

URL = "https://businessbuyers.co.uk/business/XXXXX"  # real listing

def main():
    data = scrape_bb_detail_from_page(URL)

    print("Description:", data["description"][:200])
    print("Facts:", data["facts"])
    print("Hash:", data["content_hash"][:12])
    print("HTML size:", len(data["raw_html"]))

if __name__ == "__main__":
    main()