import requests
from bs4 import BeautifulSoup
import hashlib

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def scrape_bb_detail(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    description = soup.select_one(".description, .business-description")
    description_text = description.get_text("\n", strip=True) if description else None

    facts = {}
    for row in soup.select(".key-facts li"):
        if ":" in row.text:
            k, v = row.text.split(":", 1)
            facts[k.strip().lower()] = v.strip()

    content_hash = hashlib.sha256(r.text.encode()).hexdigest()

    return {
        "description": description_text,
        "facts": facts,
        "content_hash": content_hash,
        "raw_html": r.text,
    }