from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.brokers.base import BrokerClient
from src.persistence.repository import SQLiteRepository


class BusinessBuyersClient(BrokerClient):
    BASE_URL = "https://businessbuyers.co.uk"
    selected_sector = "Healthcare"
    def __init__(self, *, username: None, password: None, login=False, click_budget):
        self.login_enabled = login
        self.username = username
        self.password = password
        self.click_budget = click_budget

        self.browser = None
        self.page = None
        self.auth_context = None
        self.anon_context = None

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

        # canonical “results ready” signals
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
            sector=self.selected_sector,
            postcode="W14 8EN",
            miles="100",
        )

        page_num = 1
        total = 0
        seen = set()

        while True:
            print(f"Scraping page {page_num}")

            cards = self.page.locator("a[href*='/business/']")
            count = cards.count()

            if count == 0:
                print("⚠️ No listing links found, stopping pagination")
                break

            for i in range(count):
                href = cards.nth(i).get_attribute("href")
                if not href:
                    continue

                if href.startswith("/"):
                    href = self.BASE_URL + href

                listing_id = href.split("/business/")[-1].strip("/")

                if listing_id in seen:
                    continue

                seen.add(listing_id)
                sector_raw = f"BusinessBuyers:{self.selected_sector}"

                self.repo.upsert_index_only(
                    source="BusinessBuyers",
                    source_listing_id=listing_id,
                    source_url=href,
                    sector_raw=sector_raw,  # broker-known, allowed
                )

                total += 1

            # ✅ pagination: ONLY real pagination next
            next_link = self.page.locator(
                "a.page-numbers.next, a[rel='next']"
            )

            if next_link.count() == 0:
                print("No pagination Next link found, stopping")
                break

            current_url = self.page.url

            next_link.first.click()
            self.page.wait_for_load_state("domcontentloaded")
            self.page.wait_for_timeout(800)
            self.ensure_cookies_cleared()

            if self.page.url == current_url:
                print("URL did not change after Next click, stopping")
                break

            page_num += 1

        print(f"Indexed {total} unique listings")

    # ------------------------------------------------------------------
    # DETAIL
    # ------------------------------------------------------------------

    def fetch_listing_detail(self, listing: dict) -> str:
        if self.click_budget is not None:
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

    def fetch_detail_anon(self, url: str) -> str:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            html = page.content()
            browser.close()
            return html

    def fetch_detail_anon_with_pdf(self, url: str, pdf_path: Path) -> str:
        # 🔑 ENSURE anon context exists
        if self.anon_context is None:
            if self.browser is None:
                # browser must exist even for anon
                self._playwright = sync_playwright().start()
                self.browser = self._playwright.chromium.launch(headless=True)

            self.anon_context = self.browser.new_context()

        page = self.anon_context.new_page()
        page.goto(url, wait_until="networkidle")

        html = page.content()

        page.pdf(
            path=str(pdf_path),
            format="A4",
            print_background=True,
            margin={
                "top": "20mm",
                "bottom": "20mm",
                "left": "15mm",
                "right": "15mm",
            },
        )

        page.close()
        return html