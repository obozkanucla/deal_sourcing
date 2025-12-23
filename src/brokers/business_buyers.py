from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.brokers.base import BrokerClient
from src.persistence.repository import SQLiteRepository
from src.integrations.google_drive import upload_pdf_to_drive

from src.integrations.google_auth import get_google_credentials
from src.integrations.google_drive import get_drive_service

class BusinessBuyersClient(BrokerClient):
    BASE_URL = "https://businessbuyers.co.uk"
    PDF_TMP_DIR = Path("tmp_pdfs")
    PDF_TMP_DIR.mkdir(exist_ok=True)

    DRIVE_FOLDER_ID = "PUT_BUSINESSBUYERS_FOLDER_ID_HERE"

    def __init__(self, username: str, password: str, click_budget):
        self.username = username
        self.password = password
        self.click_budget = click_budget

        self.browser = None
        self.page = None

        self.repo = SQLiteRepository(Path("db/deals.sqlite"))

        self.creds = get_google_credentials()
        self.drive_service = get_drive_service(self.creds)

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
            sector="Healthcare",
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

                self.repo.upsert_index_only(
                    source="BusinessBuyers",
                    source_listing_id=listing_id,
                    source_url=href,
                    sector="Healthcare",
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

    from pathlib import Path
    from datetime import datetime

    def fetch_listing_detail_and_pdf(self, listing: dict, pdf_base_dir: Path):
        self.click_budget.consume()

        url = listing["source_url"]
        deal_id = listing["deal_id"]

        self.page.goto(url)
        self.page.wait_for_load_state("networkidle")
        self.ensure_cookies_cleared()

        html = self.page.content()

        # ---- PDF ----
        pdf_dir = pdf_base_dir / listing["source"]
        pdf_dir.mkdir(parents=True, exist_ok=True)

        pdf_path = pdf_dir / f"{deal_id}.pdf"

        self.page.pdf(
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

        return {
            "raw_html": html,
            "pdf_path": str(pdf_path),
            "detail_fetched_at": datetime.utcnow().isoformat(),
        }

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

