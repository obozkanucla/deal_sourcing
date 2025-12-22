from playwright.sync_api import sync_playwright
from src.brokers.base import BrokerClient
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from datetime import datetime
from urllib.parse import urljoin

class BusinessBuyersClient(BrokerClient):

    BASE_URL = "https://businessbuyers.co.uk"

    def __init__(self, username, password, click_budget):
        self.username = username
        self.password = password
        self.click_budget = click_budget
        self.browser = None
        self.page = None

    def login(self):
        p = sync_playwright().start()
        self.browser = p.chromium.launch(headless=False, slow_mo=400)
        self.page = self.browser.new_page()

        # Load login page
        self.page.goto(f"{self.BASE_URL}/login")
        self.page.wait_for_load_state("domcontentloaded")

        # ABSOLUTE RULE: clear cookies first
        self.ensure_cookies_cleared()

        # Now and only now touch the form
        self.page.fill("input[name='log']", self.username)
        self.page.fill("input[name='pwd']", self.password)

        # Click login explicitly (do NOT rely on Enter)
        self.page.click("#wp-submit")

        # Wait for navigation
        self.page.wait_for_load_state("networkidle")
        self.page.wait_for_timeout(1000)

        # BB may re-inject cookies post-login
        self.ensure_cookies_cleared()

        if "login" in self.page.url.lower():
            raise RuntimeError("Login failed")

        print("Login successful:", self.page.url)

    def apply_sector_filter(self, sector_name: str):
        # Native <select> → use select_option
        self.page.select_option(
            "select",
            label=sector_name
        )

        # Click Search
        self.page.click("button:has-text('Search'), input[value='Search']")

        # ✅ WAIT FOR RESULTS GRID TO UPDATE (DOM-based)
        self.page.wait_for_selector(
            f"text={sector_name}",
            timeout=10000
        )

        # Extra safety
        self.page.wait_for_timeout(1000)

        self.ensure_cookies_cleared()

        print(f"Applied sector filter and refreshed results: {sector_name}")

    def fetch_index_listings(self):
        self.ensure_cookies_cleared()

        # Navigate to listings page
        self.page.click("a:has-text('Buy a business')")
        self.page.wait_for_load_state("networkidle")
        self.page.wait_for_timeout(1500)

        self.ensure_cookies_cleared()

        # ✅ Apply Healthcare filter
        self.apply_sector_filter("Healthcare")

        print("On filtered listings page:", self.page.url)

        cards = self.page.locator('a[href^="/business/"]')
        count = cards.count()

        print(f"Found {count} Healthcare listings")

        listings = []

        for i in range(count):
            card = cards.nth(i)
            href = card.get_attribute("href")
            if not href:
                continue

            listings.append({
                "source": "BusinessBuyers",
                "sector": "Healthcare",
                "source_listing_id": href,
                "source_url": self.BASE_URL + href,
                "raw_text": card.inner_text().strip(),
            })

        return listings

    def fetch_listing_detail(self, listing):
        self.click_budget.consume()

        self.page.goto(listing["source_url"])
        self.page.wait_for_load_state("networkidle")

        return self.page.content()

    def ensure_cookies_cleared(self):
        try:
            # Wait briefly for any consent UI to appear
            self.page.wait_for_timeout(1000)

            accept_buttons = self.page.get_by_role(
                "button", name="Accept All"
            )

            count = accept_buttons.count()
            if count == 0:
                return  # no cookie banner

            # Click the FIRST visible Accept All button
            accept_buttons.first.click(force=True)

            # Give JS time to process consent
            self.page.wait_for_timeout(1000)

            print("Cookie consent accepted.")

        except PlaywrightTimeoutError:
            pass