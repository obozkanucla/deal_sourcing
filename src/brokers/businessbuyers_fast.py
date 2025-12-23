import requests
from bs4 import BeautifulSoup
import hashlib

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DealBot/1.0)"
}

def scrape_bb_detail_fast(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    description_el = soup.select_one(".description, .business-description")
    description = (
        description_el.get_text("\n", strip=True)
        if description_el else None
    )

    facts = {}
    for li in soup.select(".key-facts li"):
        if ":" in li.text:
            k, v = li.text.split(":", 1)
            facts[k.strip().lower().replace(" ", "_")] = v.strip()

    content_hash = hashlib.sha256(r.text.encode()).hexdigest()

    return {
        "description": description,
        "facts": facts,
        "content_hash": content_hash,
    }