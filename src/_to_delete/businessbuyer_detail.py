# src/brokers/businessbuyers_detail.py

from bs4 import BeautifulSoup
import hashlib


def parse_bb_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # -------- description --------
    description_el = soup.select_one(
        ".description, .business-description"
    )
    description = (
        description_el.get_text("\n", strip=True)
        if description_el
        else None
    )

    # -------- key facts --------
    facts = {}
    for li in soup.select(".key-facts li"):
        text = li.get_text(strip=True)
        if ":" in text:
            k, v = text.split(":", 1)
            facts[k.strip().lower().replace(" ", "_")] = v.strip()

    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

    return {
        "description": description,
        "facts": facts,
        "content_hash": content_hash,
    }