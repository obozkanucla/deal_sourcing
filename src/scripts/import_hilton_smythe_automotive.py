import sqlite3
from pathlib import Path
from time import sleep

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

BASE_URL = "https://hiltonsmythe.com/businesses-for-sale/?business-sector=Automotive"
BROKER = "HiltonSmythe"
DRY_RUN = True


def parse_automotive_listings(html: str):
    soup = BeautifulSoup(html, "html.parser")

    cards = soup.select("article.business-listing.business-sector-automotive")

    results = []
    for card in cards:
        ref_el = card.select_one("p strong span span")
        link_el = card.select_one("a.tcb-post-read-more-link")

        if not ref_el or not link_el:
            continue

        results.append(
            {
                "ref": ref_el.get_text(strip=True),
                "url": link_el["href"],
            }
        )

    return results


def import_hilton_smythe_automotive():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    seen_refs: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(BASE_URL, timeout=30_000)
        page.wait_for_selector("article.business-listing", timeout=20_000)

        page_no = 1

        while True:
            html = page.content()
            listings = parse_automotive_listings(html)

            new = [l for l in listings if l["ref"] not in seen_refs]

            print(f"[HS] Page {page_no} → {len(new)} automotive listings")

            for item in new:
                seen_refs.add(item["ref"])

                if DRY_RUN:
                    print("DRY_RUN →", item["ref"], item["url"])
                else:
                    conn.execute(
                        """
                        INSERT INTO deals (
                            source,
                            source_listing_id,
                            source_url,
                            needs_detail_refresh,
                            first_seen,
                            last_seen
                        )
                        VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(source, source_listing_id)
                        DO UPDATE SET last_seen = CURRENT_TIMESTAMP
                        """,
                        (
                            BROKER,
                            item["ref"],
                            item["url"],
                        ),
                    )

            # find current active pagination button
            current = page.locator(".tcb-pagination-current")

            if not current.count():
                break

            next_btn = current.locator("xpath=following-sibling::*[1]")

            if not next_btn.count():
                break

            # click via JS to bypass overlay interception
            page.evaluate("(el) => el.click()", next_btn.element_handle())

            # wait until pagination-current moves
            page.wait_for_function(
                """
                (oldPage) => {
                    const cur = document.querySelector('.tcb-pagination-current');
                    return cur && cur.innerText.trim() !== oldPage;
                }
                """,
                arg=str(page_no),
                timeout=15_000,
            )

            page_no += 1

            # safety cap
            if page_no > 30:
                break

        context.close()
        browser.close()

    if not DRY_RUN:
        conn.commit()

    conn.close()

    print("\n🏁 Automotive import complete")
    print(f"Total Automotive listings captured: {len(seen_refs)}")


if __name__ == "__main__":
    import_hilton_smythe_automotive()