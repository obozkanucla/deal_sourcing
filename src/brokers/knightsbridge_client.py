import time
import random
import re
import os
from playwright.sync_api import sync_playwright
from src.config import KB_USERNAME, KB_PASSWORD

KNIGHTSBRIDGE_BASE = "https://www.knightsbridgeplc.com"
LOGIN_BASE = "https://portal.knightsbridgeplc.com/login/"

class KnightsbridgeClient:
    BASE_URL = "https://www.knightsbridgeplc.com/buy-a-business/search-our-listings/"
    SECTORS = {
        "Agriculture/Forestry/Fishing": "23",
        "Care": "1",
        "Catering": "4",
        "Child Care": "2",
        "Commercial": "3",
        "Construction": "12",
        "E-Commerce": "9",  # duplicate exists, first is fine
        "Engineering": "13",
        "Facilities & Waste Management": "15",
        "Food & Drink": "21",
        "Health & Beauty": "5",
        "Healthcare": "24",
        "Kennels": "11",
        "Leisure & Lifestyle": "22",
        "License & Leisure": "6",
        "Manufacturing": "14",
        "Miscellaneous": "26",
        "Motor Related": "10",
        "Property": "7",  # duplicate exists, first is fine
        "Retail": "8",
        "Retail/Wholesale/Distribution": "17",
        "Services": "16",
        "Technology": "19",
        "Transport/Logistics/Storage": "18",
    }
    BASE_SLEEP = 1.2
    JITTER = 0.8

    RESTART_EVERY = 50
    BASE_SLEEP = 1.2
    JITTER = 0.8


    # Proven sector values
    def __init__(self):
        self.browser = None
        self.page = None
        self._playwright = None
        self.HEADLESS = False # os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
    # ------------------------------------------------------------------
    # LIFECYCLE
    # ------------------------------------------------------------------

    def _pre_accept_cookies(self):
        self.context.add_cookies([
            {
                "name": "CookieConsent",
                "value": "{stamp:'accepted',necessary:true,preferences:true,statistics:true,marketing:true}",
                "domain": "www.knightsbridgeplc.com",
                "path": "/",
            }
        ])


    def start(self):
        print("🚀 Starting Knightsbridge client (headless =", self.HEADLESS, ")")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(
            headless=self.HEADLESS,
            slow_mo=100  # optional, highly recommended for observing Cookiebot
        )
        self.context = self.browser.new_context()
        self._pre_accept_cookies()
        self.page = self.context.new_page()

    def stop(self):
        print("🛑 Stopping Knightsbridge client")
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()

    def login(self):
        print("🔐 Logging into Knightsbridge")

        self.page.goto(
            LOGIN_BASE,
            wait_until="networkidle",
            timeout=30_000,
        )

        # --- HARD WAIT: inputs must exist and be visible ---
        self.page.wait_for_selector(
            "input#LoginEmail",
            state="visible",
            timeout=30_000,
        )
        self.page.wait_for_selector(
            "input#LoginPassword",
            state="visible",
            timeout=30_000,
        )

        # Fill credentials
        self.page.fill("input#LoginEmail", KB_USERNAME)
        self.page.fill("input#LoginPassword", KB_PASSWORD)

        # Submit via the actual onclick handler
        self.page.evaluate("LoginUser('#ContentPlaceHolder1_ctl08')")

        # Wait for post-login navigation
        self.page.wait_for_load_state("networkidle")
        self.page.wait_for_timeout(1500)

        # Login success assertion (robust)
        if (
                "login" in self.page.url.lower()
                or self.page.locator("text=Forgot password").count() > 0
        ):
            raise RuntimeError("Knightsbridge login failed")

        print("✅ Logged in successfully")

    def _accept_cookies_if_present(self):
        try:
            # Cookiebot lives in an iframe
            frame = self.page.frame_locator("iframe[src*='consent']")
            btn = frame.locator(
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"
            )
            if btn.count() > 0:
                btn.click()
                self.page.wait_for_timeout(1500)
                print("🍪 Accepted cookies (Cookiebot iframe)")
        except Exception:
            pass

    def _human_sleep(self, min_extra=0.0):
        time.sleep(self.BASE_SLEEP + min_extra + random.random() * self.JITTER)

    # ------------------------------------------------------------------
    # INDEX SCRAPE (VISIBLE CARDS ONLY)
    # ------------------------------------------------------------------

    def fetch_index(self) -> list[dict]:
        if not self.page:
            raise RuntimeError("Client not started")

        all_rows: dict[str, dict] = {}

        for sector_name, sector_value in self.SECTORS.items():
            print(f"\n🧭 Sector: {sector_name}")

            page_no = 1

            while True:
                url = (
                    f"{self.BASE_URL}"
                    f"?sector={sector_value}"
                    f"&PageNumber={page_no}"
                )

                self.page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                self._human_sleep(0.8)

                cards = self.page.locator("div.wp-block-post.unb-business-listing")
                card_count = cards.count()

                if card_count == 0:
                    break

                new_rows = 0

                for i in range(card_count):
                    card = cards.nth(i)

                    ref_text = card.locator("p.reference").inner_text()
                    m = re.search(r"Ref:\s*(\d+)", ref_text)
                    if not m:
                        continue

                    listing_id = m.group(1)
                    if listing_id in all_rows:
                        continue

                    title = (
                        card.locator("h4.wp-block-post-title")
                        .inner_text()
                        .strip()
                    )

                    href = card.locator("a.wp-block-read-more").get_attribute("href")
                    if not href:
                        continue

                    all_rows[listing_id] = {
                        "source": "Knightsbridge",
                        "source_listing_id": listing_id,
                        "source_url": href,
                        "title": title,
                        "sector_raw": sector_name,
                    }

                    new_rows += 1
                    print(f"✅ DEAL {listing_id} | {title}")

                if new_rows == 0:
                    break

                page_no += 1
                self._human_sleep(0.6)

        print(f"\n🏁 Knightsbridge index scrape complete — {len(all_rows)} deals")
        return list(all_rows.values())