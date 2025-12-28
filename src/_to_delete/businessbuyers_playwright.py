from playwright.sync_api import sync_playwright

def scrape_bb_detail_full(url: str) -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(url, timeout=60_000)
        page.wait_for_load_state("networkidle")

        html = page.content()
        title = page.title()

        browser.close()

    return {
        "raw_html": html,
        "title": title,
    }