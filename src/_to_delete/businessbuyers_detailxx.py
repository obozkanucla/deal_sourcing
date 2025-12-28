import hashlib
from bs4 import BeautifulSoup
from playwright.sync_api import Page

def scrape_bb_detail_from_page(page: Page, url: str) -> dict:
    """
    Uses an already-logged-in Playwright page.
    Parsing logic is IDENTICAL to the old requests-based version.
    """

    page.goto(url, timeout=60_000)
    page.wait_for_load_state("networkidle")

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    description = soup.select_one(
        ".description, .business-description"
    )
    description_text = (
        description.get_text("\n", strip=True)
        if description else None
    )

    facts = {}
    for row in soup.select(".key-facts li"):
        if ":" in row.text:
            k, v = row.text.split(":", 1)
            facts[k.strip().lower()] = v.strip()

    content_hash = hashlib.sha256(
        html.encode("utf-8")
    ).hexdigest()

    return {
        "description": description_text,
        "facts": facts,
        "content_hash": content_hash,
        "raw_html": html,
    }