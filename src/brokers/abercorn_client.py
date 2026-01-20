# src/brokers/abercorn_client.py

import time
import random
import re
from playwright.sync_api import sync_playwright

ABERCORN_BASE = "https://abercornbusinesssales.com"
INDEX_URL = f"{ABERCORN_BASE}/businesses-for-sale-sector/ECA"

BASE_SLEEP = 1.0
JITTER = 0.6


class AbercornClient:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
        self._playwright = None

    # ------------------------------------------------------------------
    # LIFECYCLE
    # ------------------------------------------------------------------

    def start(self):
        print("🚀 Starting Abercorn client (headless =", self.headless, ")")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context()
        self.page = self.context.new_page()

    def stop(self):
        print("🛑 Stopping Abercorn client")
        try:
            if self.browser:
                self.browser.close()
        finally:
            if self._playwright:
                self._playwright.stop()

    def _human_sleep(self, extra: float = 0.0):
        time.sleep(BASE_SLEEP + extra + random.random() * JITTER)

    # ------------------------------------------------------------------
    # INDEX SCRAPE (SINGLE PASS)
    # ------------------------------------------------------------------

    def fetch_index(self) -> list[dict]:
        if not self.page:
            raise RuntimeError("Client not started")

        print(f"🌐 Fetching Abercorn index: {INDEX_URL}")
        self.page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=30_000)
        self._human_sleep(1.0)

        cards = self.page.locator("div.row.listing-row")
        count = cards.count()

        print(f"🔎 Found {count} cards")

        rows: dict[str, dict] = {}

        for i in range(count):
            card = cards.nth(i)

            try:
                link = card.locator("a").first
                href = link.get_attribute("href")
                title = card.locator("h2").inner_text().strip()
                ref_text = card.locator("h3").inner_text()

                if not href or not title:
                    continue

                # Extract REF like "ECA-029", "RA05-ABS", etc
                m = re.search(r"Ref\s*No\s*:\s*([A-Z0-9\-]+)", ref_text)
                if not m:
                    continue

                listing_id = m.group(1)

                if listing_id in rows:
                    continue

                full_url = (
                    href if href.startswith("http")
                    else f"{ABERCORN_BASE}/{href.lstrip('/')}"
                )

                rows[listing_id] = {
                    "source": "Abercorn",
                    "source_listing_id": listing_id,
                    "source_url": full_url,
                    "title": title,
                }

                print(f"✅ DEAL {listing_id} | {title[:60]}")

            except Exception:
                continue

        print(f"\n🏁 Abercorn index scrape complete — {len(rows)} deals")
        return list(rows.values())