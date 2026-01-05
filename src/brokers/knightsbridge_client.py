import time
import random
import re
import os
from playwright.sync_api import sync_playwright



class KnightsbridgeClient:
    BASE_URL = "https://www.knightsbridgeplc.com/buy-a-business/commercial/"
    SECTORS = {
        "Advertising & Media": "2403",
        "Architecture": "2458",
        "Audio & Visual": "2452",
        "Breweries & Distilleries": "2464",
        "Business Services": "2238",
        "Civil Engineering": "2404",
        "Cleaning Company": "2405",
        "Construction & Building": "2239",
        "Consultancy": "2460",
        "Corporate": "2406",
        "E-commerce": "2418",
        "Education Services": "2440",
        "Electrical/Electricians": "2455",
        "Engineering": "2240",
        "Environmental/Energy": "2450",
        "Events": "2461",
        "Fabrications": "2407",
        "Facilities Management": "2463",
        "Financial Services": "2408",
        "Fire & Security": "2462",
        "Flooring Services": "2459",
        "Food & Drink": "2437",
        "Food Related": "2241",
        "Funeral Services": "2410",
        "Gardening & Landscaping": "2454",
        "Glass Related": "2456",
        "Health & Safety": "2448",
        "Import & Distribution": "2242",
        "IT Consultancy": "2443",
        "IT Services & Support": "2444",
        "IT Technology & Web": "2243",
        "Leisure & Lifestyle": "2411",
        "Machinery": "2447",
        "Manufacturing": "2244",
        "Medical & Education": "2412",
        "Medical Service": "2441",
        "Miscellaneous": "2413",
        "Mobility Equipment": "2453",
        "Plumbing & Heating": "2451",
        "Print, Publishing, Media & Marketing": "2245",
        "Professional & Financial Services": "2246",
        "Professional & Legal Services": "2438",
        "Recruitment": "2247",
        "Refrigeration & Air Conditioning": "2414",
        "Removals & Storage": "2439",
        "School/Training Centre": "2465",
        "Service": "2415",
        "Service (Other)": "2416",
        "Software": "2442",
        "Telecommunications": "2445",
        "Training": "2449",
        "Transport, Haulage & Logistics": "2248",
        "Vending Rounds": "2417",
        "Waste Management & Recycling": "2249",
        "Web Design & Development": "2446",
        "Wholesale & Retail": "2250",
        "Windows & Doors": "2457",
    }
    # HEADLESS = False
    HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
    BASE_SLEEP = 1.2
    JITTER = 0.8

    # Proven sector values
    def __init__(self):
        self.browser = None
        self.page = None
        self._playwright = None
        self.HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
    # ------------------------------------------------------------------
    # LIFECYCLE
    # ------------------------------------------------------------------

    def start(self):
        print("🚀 Starting Knightsbridge client")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=self.HEADLESS)
        self.page = self.browser.new_page()

    def stop(self):
        print("🛑 Stopping Knightsbridge client")
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()

    def _human_sleep(self, min_extra=0.0):
        time.sleep(self.BASE_SLEEP + min_extra + random.random() * self.JITTER)

    # ------------------------------------------------------------------
    # INDEX SCRAPE (VISIBLE CARDS ONLY)
    # ------------------------------------------------------------------

    def fetch_index(self) -> list[dict]:
        if not self.page:
            raise RuntimeError("Client not started")

        print("📄 Loading Knightsbridge commercial buying page")
        self.page.goto(self.BASE_URL, timeout=30_000)
        self.page.wait_for_load_state("domcontentloaded")
        self._human_sleep(1.0)

        # Guard: error page
        body_text = self.page.locator("body").inner_text().lower()
        if "sorry" in body_text and "cannot be displayed" in body_text:
            raise RuntimeError("Knightsbridge error page detected — aborting")

        all_rows: dict[str, dict] = {}

        for sector_name, sector_value in self.SECTORS.items():
            print(f"\n🧭 Sector: {sector_name}")

            # --------------------------------------------------
            # Select sector
            # --------------------------------------------------
            self.page.select_option(
                "#ContentPlaceHolder1_ctl09_subSector",
                value=sector_value,
            )
            self._human_sleep(0.5)

            # --------------------------------------------------
            # Search
            # --------------------------------------------------
            self.page.evaluate("Search('#ContentPlaceHolder1_ctl09', 1);")
            self.page.wait_for_selector(
                "div.business-listing.commercial",
                timeout=30_000,
            )
            self._human_sleep(1.0)

            page_no = 1

            while True:
                cards = self.page.locator("div.business-listing.commercial")
                card_count = cards.count()
                if card_count == 0:
                    break

                new_rows = 0

                for i in range(card_count):
                    card = cards.nth(i)

                    title = card.locator("h3").inner_text().strip()
                    ref_text = card.locator("p.ref").inner_text().strip()

                    m = re.search(r"REF:\s*(\d+)", ref_text)
                    if not m:
                        continue

                    listing_id = m.group(1)
                    if listing_id in all_rows:
                        continue

                    href = card.locator("a.btn.details").get_attribute("href")
                    if not href:
                        continue

                    all_rows[listing_id] = {
                        "source": "Knightsbridge",
                        "source_listing_id": listing_id,
                        "source_url": f"https://www.knightsbridgeplc.com{href}",
                        "title": title,
                        "sector_raw": sector_name,
                    }

                    new_rows += 1
                    print(f"✅ DEAL {listing_id} | {title}")

                if new_rows == 0:
                    break

                page_no += 1
                self.page.evaluate(
                    f"setCurrentIndex('#ContentPlaceHolder1_ctl13', {page_no});"
                )
                self._human_sleep(1.2)

                try:
                    self.page.wait_for_selector(
                        "div.business-listing.commercial",
                        timeout=15_000,
                    )
                except Exception:
                    break

        print(f"\n🏁 Knightsbridge index scrape complete — {len(all_rows)} deals")
        return list(all_rows.values())