# src/brokers/businesses4sale_client.py

import os
import time
import random
from pathlib import Path
from typing import List, Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

BASE_URL = "https://uk.businessesforsale.com/uk/m-and-a-vault"
STORAGE_STATE = Path(".playwright/businesses4sale_state.json")


class BusinessesForSaleClient:
    def __init__(
        self,
        *,
        headless: bool = False,
        slow_mo_ms: int = 0,
        max_pages: int | None = None,
        sleep_between_pages: float = 3.0,
    ):
        self.headless = headless
        self.slow_mo_ms = slow_mo_ms
        self.max_pages = max_pages
        self.sleep_between_pages = sleep_between_pages

    # =================================================
    # Public API
    # =================================================

    def open_session(self):
        p = sync_playwright().start()
        browser = p.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo_ms,
        )
        context = self._create_context(browser)
        page = context.new_page()

        return {
            "playwright": p,
            "browser": browser,
            "context": context,
            "page": page,
        }

    def close_session(self, session):
        session["context"].storage_state(path=STORAGE_STATE)
        session["browser"].close()
        session["playwright"].stop()

    def fetch_index(self) -> list[dict]:
        listings: dict[str, dict] = {}

        page_num = 1

        while True:
            if page_num == 1:
                url = BASE_URL
            else:
                url = f"{BASE_URL}-{page_num}"

            print(f"\n🔍 PAGE {page_num}: {url}")

            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=self.headless,
                    slow_mo=self.slow_mo_ms,
                )

                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )

                page = context.new_page()
                page.goto(url, timeout=60_000)

                try:
                    page.wait_for_selector("div.mv-results", timeout=15_000)
                except TimeoutError:
                    print("🛑 Page blocked or empty — stopping crawl")
                    browser.close()
                    break

                soup = BeautifulSoup(page.content(), "html.parser")
                blocks = soup.select("div.mv-result")
                print(f"🔍 mv-result blocks found: {len(blocks)}")

                added = 0
                for block in blocks:
                    rec = self._parse_index_block(block)
                    if not rec:
                        continue

                    key = rec["source_listing_id"]
                    if key not in listings:
                        listings[key] = rec
                        added += 1

                print(f"➕ New listings on page: {added}")
                browser.close()

            if added == 0:
                print("⛔ No new listings — pagination exhausted")
                break

            if self.max_pages and page_num >= self.max_pages:
                print("🛑 Max pages reached — stopping")
                break

            page_num += 1
            time.sleep(self.sleep_between_pages + random.random() * 3)

        print(f"\n✅ Total unique listings extracted: {len(listings)}")
        return list(listings.values())

    # =================================================
    # Cloudflare handling
    # =================================================

    def _handle_cloudflare(self, page):
        """
        Handle Cloudflare challenge on initial page load.
        """
        try:
            page.wait_for_selector("div.mv-results", timeout=10_000)
            return
        except TimeoutError:
            pass

        print("\n🛑 Cloudflare challenge detected.")
        print("👉 Please complete verification in the browser window.")
        input("Press ENTER here once verification is complete...")

        page.reload(timeout=60_000)
        page.wait_for_selector("div.mv-results", timeout=60_000)

        print("✅ Cloudflare cleared — session will be reused")

    def _wait_for_results_or_challenge(self, page):
        """
        Safe wait strategy during pagination.
        """
        try:
            page.wait_for_selector("div.mv-results", timeout=20_000)
            return
        except TimeoutError:
            pass

        html = page.content()
        if "Verify you are human" in html:
            print("🛑 Cloudflare challenge detected mid-pagination.")
            print("👉 Please complete verification in the browser.")
            input("Press ENTER here once verification is complete...")

            page.reload(timeout=60_000)
            page.wait_for_selector("div.mv-results", timeout=60_000)
            print("✅ Resuming after Cloudflare clearance")
            return

        raise RuntimeError("Page neither loaded listings nor showed Cloudflare challenge")

    # =================================================
    # Browser context
    # =================================================

    def _create_context(self, browser):
        if STORAGE_STATE.exists():
            print("🔐 Reusing saved Cloudflare session")
            return browser.new_context(storage_state=STORAGE_STATE)

        print("🧠 No session found — new context")
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

    def _parse_index_block(self, block) -> dict | None:
        a = block.select_one("h2 a")
        if not a:
            return None

        url = a.get("href")
        if not url:
            return None

        slug = url.rstrip("/").split("/")[-1]

        return {
            "source": "BusinessesForSale",
            "source_listing_id": slug,
            "source_url": url,
            "title": a.get_text(strip=True),
        }