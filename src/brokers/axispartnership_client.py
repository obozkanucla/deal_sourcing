import time
import os
import random
import re
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


class AxisPartnershipClient:
    BASE_URL = "https://www.axispartnership.co.uk/buying/"
    HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
    BASE_SLEEP = 1.5
    JITTER = 1.0

    def __init__(self):
        self.browser = None
        self.page = None
        self._playwright = None

    # ------------------------------------------------------------------
    # LIFECYCLE
    # ------------------------------------------------------------------

    def start(self):
        print("🚀 Starting Axis Partnership client")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=self.HEADLESS)
        self.page = self.browser.new_page()

    def stop(self):
        print("🛑 Stopping Axis Partnership client")
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()

    def _human_sleep(self, extra=0.0):
        time.sleep(self.BASE_SLEEP + extra + random.random() * self.JITTER)

    # ------------------------------------------------------------------
    # INDEX SCRAPE — ALL GRIDS, ALL LOAD MORE
    # ------------------------------------------------------------------

    def fetch_index(self) -> list[dict]:
        if not self.page:
            raise RuntimeError("Client not started")

        print("📄 Loading Axis buying page")
        self.page.goto(self.BASE_URL, timeout=30_000)
        self.page.wait_for_load_state("domcontentloaded")
        self._human_sleep(1.0)

        rows = {}

        grids = self.page.locator("div.cz_grid")
        grid_count = grids.count()
        print(f"🔎 Found {grid_count} category grids")

        for grid_idx in range(grid_count):
            grid = grids.nth(grid_idx)
            print(f"\n📦 Exhausting grid {grid_idx + 1}")

            # ---- HARD exhaust Load More for this grid ----
            # ---- Exhaust Load More for this grid (Codevz-compatible) ----
            while True:
                pager = grid.locator(
                    "xpath=following-sibling::div[contains(@class,'cz_ajax')]//a"
                )

                if pager.count() == 0:
                    break

                text = pager.first.inner_text().strip().lower()
                if "not found more posts" in text:
                    break

                print("➡️ Clicking Load More")
                try:
                    pager.first.click(force=True, timeout=5_000)
                    self._human_sleep(1.5)
                except Exception:
                    break

            cards = grid.locator(".cz_grid_item[data-id]")
            card_count = cards.count()
            print(f"🔍 Found {card_count} cards in grid {grid_idx + 1}")

            for i in range(card_count):
                card = cards.nth(i)

                link = card.locator("a.cz_grid_link")
                href = link.get_attribute("href")
                title = card.locator("h3").inner_text().strip()

                if not href or not title:
                    continue

                m = re.search(r"(\d{4})$", title)
                if not m:
                    continue

                listing_id = m.group(1)

                rows[listing_id] = {
                    "source": "AxisPartnership",
                    "source_listing_id": listing_id,
                    "source_url": href,
                    "title": title,
                    "sector_raw": "Healthcare",
                }

                print(f"✅ DEAL {listing_id} | {title}")

        print(f"\n🏁 Axis index scrape complete — {len(rows)} deals")
        return list(rows.values())

    # ------------------------------------------------------------------
    # DETAIL + PDF
    # ------------------------------------------------------------------

    def fetch_detail_and_pdf(self, url: str, pdf_path: Path) -> str:
        if not self.page:
            raise RuntimeError("Client not started")

        print(f"\n➡️ Fetching Axis detail page\n{url}")
        self.page.goto(url, timeout=30_000)
        self.page.wait_for_load_state("networkidle")
        self._human_sleep(1.0)

        html = self.page.content()

        pdf_path.parent.mkdir(parents=True, exist_ok=True)
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

        print("📄 PDF captured")
        return html


    def parse_detail(self, html: str) -> dict:
        """
        Parse Axis Partnership deal detail page.
        Assumes full page HTML.
        """

        soup = BeautifulSoup(html, "html.parser")

        # ----------------------------------------------------------
        # STATUS (FOR SALE vs UNDER OFFER)
        # ----------------------------------------------------------
        status = "for_sale"
        title_el = soup.select_one("h1, h3")
        if title_el and "under offer" in title_el.get_text(strip=True).lower():
            status = "under_offer"

        # ----------------------------------------------------------
        # ASKING PRICE
        # ----------------------------------------------------------
        asking_price_k = None
        price_el = soup.select_one(".cz_post_data.cz_data_custom_meta i.czico-071-money-3")
        if price_el:
            parent = price_el.find_parent("span")
            if parent:
                asking_price_k = parent.get_text(strip=True)

        # ----------------------------------------------------------
        # LOCATION
        # ----------------------------------------------------------
        location = None
        loc_el = soup.select_one(".cz_post_data.cz_data_custom_meta i.czico-082-maps-and-flags")
        if loc_el:
            parent = loc_el.find_parent("span")
            if parent:
                location = parent.get_text(strip=True)

        # ----------------------------------------------------------
        # DESCRIPTION (MAIN BODY)
        # ----------------------------------------------------------
        description_parts = []

        excerpt = soup.select_one(".cz_post_excerpt")
        if excerpt:
            description_parts.append(excerpt.get_text(" ", strip=True))

        # Sometimes full body continues below excerpt
        content = soup.select_one(".entry-content, .post-content")
        if content:
            paragraphs = content.find_all("p")
            for p in paragraphs:
                txt = p.get_text(" ", strip=True)
                if txt and txt not in description_parts:
                    description_parts.append(txt)

        description = "\n\n".join(description_parts) if description_parts else None

        # ----------------------------------------------------------
        # TURNOVER & EBITDA (BEST-EFFORT, NON-DESTRUCTIVE)
        # ----------------------------------------------------------
        turnover = None
        ebitda = None

        text_blob = soup.get_text(" ", strip=True)

        t_match = re.search(r"Turnover[^£]*£[\d,.]+", text_blob, re.IGNORECASE)
        if t_match:
            turnover = t_match.group(0)

        e_match = re.search(r"EBITDA[^£]*£[\d,.]+", text_blob, re.IGNORECASE)
        if e_match:
            ebitda = e_match.group(0)

        # ----------------------------------------------------------
        # RETURN NORMALISED STRUCTURE
        # ----------------------------------------------------------
        facts = {}

        if asking_price_k:
            facts["asking_price_k"] = asking_price_k
        if location:
            facts["location"] = location
        if turnover:
            facts["revenue_k"] = turnover
        if ebitda:
            facts["ebitda_k"] = ebitda
        if status:
            facts["status"] = status

        return {
            "description": description,
            "facts": facts,
        }