from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.brokers.base import BrokerClient
from src.persistence.repository import SQLiteRepository


class BusinessBuyersClient(BrokerClient):
    BASE_URL = "https://businessbuyers.co.uk"

    def __init__(self, username: str, password: str, click_budget):
        self.username = username
        self.password = password
        self.click_budget = click_budget

        self.browser = None
        self.page = None

        self.repo = SQLiteRepository(Path("db/deals.sqlite"))

    # ------------------------------------------------------------------
    # AUTH
    # ------------------------------------------------------------------

    def login(self):
        p = sync_playwright().start()

        self.browser = p.chromium.launch(
            headless=False,
            slow_mo=400,
        )

        context = self.browser.new_context()
        context.grant_permissions([], origin=self.BASE_URL)

        self.page = context.new_page()

        self.page.goto(f"{self.BASE_URL}/login")
        self.page.wait_for_load_state("domcontentloaded")

        self.ensure_cookies_cleared()

        self.page.fill("input[name='log']", self.username)
        self.page.fill("input[name='pwd']", self.password)
        self.page.click("#wp-submit")

        self.page.wait_for_load_state("networkidle")
        self.page.wait_for_timeout(1000)

        self.ensure_cookies_cleared()

        if "login" in self.page.url.lower():
            raise RuntimeError("Login failed")

        print("Login successful:", self.page.url)

    # ------------------------------------------------------------------
    # SEARCH / INDEX
    # ------------------------------------------------------------------

    def apply_search_filters(
        self,
        *,
        sector: str,
        postcode: str = "W14 8EN",
        miles: str = "100",
    ):
        self.page.select_option("select", label=sector)
        self.page.locator("input[placeholder*='Postcode']").fill(postcode)
        self.page.select_option("#gmw_distance", label=miles)

        self.page.click("button:has-text('Search'), input[value='Search']")

        # canonical “results ready” signal
        self.page.wait_for_url("**/search-results/**", timeout=15000)
        self.page.wait_for_selector("text=Showing", timeout=15000)
        self.page.wait_for_timeout(800)

        self.ensure_cookies_cleared()

        print(
            f"Applied search filters: sector={sector}, "
            f"postcode={postcode}, miles={miles}"
        )

    def fetch_index_listings(self):
        self.ensure_cookies_cleared()

        self.page.click("a:has-text('Buy a business')")
        self.page.wait_for_load_state("domcontentloaded")
        self.ensure_cookies_cleared()

        self.apply_search_filters(
            sector="Healthcare",
            postcode="W14 8EN",
            miles="100",
        )

        seen: set[str] = set()
        page_num = 1
        total = 0

        while True:
            print(f"Scraping page {page_num}")

            self.page.wait_for_selector("text=Showing", timeout=15000)

            cards = self.page.locator("text=REF:").locator("xpath=ancestor::a[1]")
            count = cards.count()

            if count == 0:
                break

            for i in range(count):
                href = cards.nth(i).get_attribute("href")
                if not href or href in seen:
                    continue

                seen.add(href)

                self.repo.upsert_index_only(
                    source="BusinessBuyers",
                    source_listing_id=href,
                    source_url=self.BASE_URL + href,
                    sector="Healthcare",
                )

                total += 1

            next_btn = self.page.locator("a:has-text('Next')")
            if next_btn.count() == 0:
                break

            classes = next_btn.first.get_attribute("class") or ""
            if "disabled" in classes.lower():
                break

            next_btn.first.click()
            self.page.wait_for_load_state("domcontentloaded")
            self.page.wait_for_timeout(800)

            self.ensure_cookies_cleared()
            page_num += 1

        print(f"Indexed {total} unique listings")

    # ------------------------------------------------------------------
    # DETAIL
    # ------------------------------------------------------------------

    def fetch_listing_detail(self, listing: dict) -> str:
        self.click_budget.consume()

        self.page.goto(listing["source_url"])
        self.page.wait_for_load_state("networkidle")

        return self.page.content()

    # ------------------------------------------------------------------
    # UTIL
    # ------------------------------------------------------------------

    def ensure_cookies_cleared(self):
        try:
            self.page.wait_for_timeout(800)

            accept_buttons = self.page.get_by_role(
                "button",
                name="Accept All",
            )

            if accept_buttons.count() == 0:
                return

            accept_buttons.first.click(force=True)
            self.page.wait_for_timeout(800)

            print("Cookie consent accepted.")

        except PlaywrightTimeoutError:
            pass