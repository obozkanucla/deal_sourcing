import time
import random
from playwright.sync_api import sync_playwright
from pathlib import Path

class DealOpportunitiesClient:
    # =========================
    # CONFIG
    # =========================

    BASE_URL = "https://www.dealopportunities.co.uk/search/advanced"

    HEADLESS = False
    MAX_PAGES_PER_RUN = 50
    BASE_SLEEP = 3
    JITTER = 3

    # =========================
    # LIFECYCLE
    # =========================

    def __init__(self):
        self.browser = None
        self.page = None
        self._playwright = None

    def start(self):
        print("🚀 Starting DealOpportunities client")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(
            headless=self.HEADLESS
        )
        self.page = self.browser.new_page()

    def stop(self):
        print("🛑 Stopping DealOpportunities client")
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()

    # =========================
    # HELPERS
    # =========================

    def _human_sleep(self):
        time.sleep(self.BASE_SLEEP + random.random() * self.JITTER)

    def _extract_dl_map(self, li):
        data = {}

        dts = li.query_selector_all("dl dt")
        for dt in dts:
            label = dt.inner_text().strip().lower()
            dd = dt.evaluate_handle("el => el.nextElementSibling")
            value = dd.inner_text().strip() if dd else None

            if "sector" in label:
                data["sectors"] = value
            elif "region" in label:
                data["regions"] = value
            elif "turnover" in label:
                data["turnover"] = value
            elif "offers required" in label:
                data["deadline"] = value

        return data

    # =========================
    # INDEX SCRAPE
    # =========================

    def fetch_index(self, max_pages: int = None) -> list[dict]:
        if not self.page:
            raise RuntimeError("Client not started. Call start().")

        rows = []
        max_pages = max_pages or self.MAX_PAGES_PER_RUN

        print(f"📄 Loading DealOpportunities search page")
        self.page.goto(self.BASE_URL, timeout=30_000)
        self.page.wait_for_load_state("domcontentloaded")

        try:
            self.page.click("text=I agree", timeout=3_000)
            print("🍪 Cookie consent accepted")
        except Exception:
            pass

        self._human_sleep()

        print("🔎 Submitting advanced search")
        self.page.evaluate("document.querySelector('form').submit();")
        self.page.wait_for_selector("section.left.listings h1", timeout=20_000)

        page_num = 1
        total = 0

        while True:
            print(f"\n📄 Scraping page {page_num}")

            items = self.page.query_selector_all("ul.clearfix > li")
            print(f"🔍 Found {len(items)} listings")

            for li in items:
                title_el = li.query_selector("h2 a")
                if not title_el:
                    continue

                ref_el = li.query_selector("span.ref")
                ref = ref_el.inner_text().strip("()") if ref_el else None

                meta = self._extract_dl_map(li)

                rows.append({
                    "source": "DealOpportunities",
                    "source_listing_id": ref,
                    "source_url": title_el.get_attribute("href"),
                    "title": title_el.inner_text().strip(),
                    "sectors_multi": meta.get("sectors"),
                    "location": meta.get("regions"),
                    "revenue": meta.get("turnover"),
                    "deadline": meta.get("deadline"),
                })

                total += 1

            print(f"📊 Total collected so far: {total}")

            if page_num >= max_pages:
                print("🛑 Page limit reached")
                break

            next_btn = self.page.query_selector("a.page-next")
            if not next_btn:
                print("🏁 No next page — stopping")
                break

            next_btn.scroll_into_view_if_needed()
            self._human_sleep()
            next_btn.click()

            self.page.wait_for_selector(
                "ul.clearfix > li h2 a",
                timeout=20_000
            )
            self._human_sleep()

            page_num += 1

        print(f"\n✅ Index scrape complete — {total} listings collected")
        return rows

    # =========================
    # DETAIL SCRAPE
    # =========================

    def fetch_listing_detail(self, url: str) -> str:
        if not self.page:
            raise RuntimeError("Client not started. Call start().")

        print(f"➡️ Fetching detail page:\n   {url}")
        self.page.goto(url, timeout=30_000)
        self.page.wait_for_load_state("networkidle")

        html = self.page.content()
        print("✅ Detail HTML captured")

        return html

    def fetch_listing_detail_and_pdf(self, url: str, pdf_path: Path) -> str:
        print("➡️ Fetching detail page:")
        print(f"   {url}")

        self.page.goto(url, timeout=60_000)
        self.page.wait_for_load_state("networkidle")

        html = self.page.content()

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

        print("✅ Detail HTML + PDF captured")
        return html