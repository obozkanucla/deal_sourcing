import os
import time
import random
from pathlib import Path
from typing import Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

BASE_URL = "https://uk.businessesforsale.com/uk/search/businesses-for-sale"
STORAGE_STATE = Path(".playwright/businesses4sale_search_state.json")
STORAGE_STATE.parent.mkdir(parents=True, exist_ok=True)


class BusinessesForSaleSearchClient:
    def __init__(
        self,
        *,
        max_pages: int | None = None,
        sleep_between_pages: float = 3.0,
    ):
        self.headless = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
        self.max_pages = max_pages
        self.sleep_between_pages = sleep_between_pages

    # =================================================
    # Public API
    # =================================================

    def fetch_index(self) -> list[dict]:
        listings: Dict[str, dict] = {}
        page_num = 1

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)

            context = self._create_context(browser)
            page = context.new_page()

            while True:
                url = (
                    BASE_URL
                    if page_num == 1
                    else f"{BASE_URL}-{page_num}"
                )

                print(f"\n🔍 PAGE {page_num}: {url}")
                page.goto(url, timeout=60_000, wait_until="domcontentloaded")

                self._wait_for_results_or_challenge(page)

                soup = BeautifulSoup(page.content(), "html.parser")
                blocks = soup.select("div.search-result")

                print(f"🔎 search-result blocks found: {len(blocks)}")

                if page_num == 1 and len(blocks) < 10:
                    raise RuntimeError("Unexpectedly few results — selector likely wrong")

                added = 0
                for block in blocks:
                    rec = self._parse_block(block)
                    if not rec:
                        continue

                    key = rec["source_listing_id"]
                    if key not in listings:
                        listings[key] = rec
                        added += 1

                print(f"➕ New listings: {added}")

                if added == 0:
                    print("⛔ Pagination exhausted")
                    break

                if self.max_pages and page_num >= self.max_pages:
                    print("🛑 Max pages reached")
                    break

                page_num += 1
                time.sleep(self.sleep_between_pages + random.random() * 2)

            context.storage_state(path=STORAGE_STATE)
            browser.close()

        print(f"\n✅ Total unique listings: {len(listings)}")
        return list(listings.values())

    # =================================================
    # Cloudflare handling
    # =================================================

    def _wait_for_results_or_challenge(self, page):
        try:
            page.wait_for_selector("div.search-result", timeout=15_000)
            return
        except TimeoutError:
            pass

        if "Verify you are human" in page.content():
            print("🛑 Cloudflare challenge detected.")
            print("👉 Complete verification in browser.")
            input("Press ENTER once done...")

            page.reload(timeout=60_000)
            page.wait_for_selector("div.search-result", timeout=60_000)
            print("✅ Cloudflare cleared")
            return

        raise RuntimeError("Search page failed to load")

    # =================================================
    # Context
    # =================================================

    def _create_context(self, browser):
        if STORAGE_STATE.exists():
            print("🔐 Reusing Cloudflare session (search)")
            return browser.new_context(storage_state=STORAGE_STATE)

        print("🧠 New browser context (search)")
        return browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

    # =================================================
    # Parsing
    # =================================================

    def _parse_block(self, block) -> dict | None:
        a = block.select_one("h2 a")
        if not a:
            return None

        url = a.get("href")
        if not url:
            return None

        slug = url.rstrip("/").split("/")[-1]

        return {
            "source": "BusinessesForSale_Search",
            "source_listing_id": slug,
            "source_url": url,
            "title": a.get_text(strip=True),
        }