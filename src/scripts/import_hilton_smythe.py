# src/scripts/import_hilton_smythe.py

import sqlite3
from pathlib import Path
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.sector_mappings.hiltonsmythe import HILTON_SMYTHE_SECTOR_MAP

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

BASE_URL = "https://hiltonsmythe.com/businesses-for-sale/"
BROKER = "HiltonSmythe"
DRY_RUN = False


def sector_tokens(sector_label: str) -> list[str]:
    return [
        t.lower()
        for t in sector_label
        .replace("&", "")
        .replace(",", "")
        .split()
        if len(t) > 2
    ]


def parse_sector_listings(html: str, sector_label: str):
    soup = BeautifulSoup(html, "html.parser")

    tokens = sector_tokens(sector_label)
    cards = []

    for card in soup.select("article.business-listing"):
        classes = card.get("class", [])
        sector_classes = [c for c in classes if c.startswith("business-sector-")]

        if any(token in c for c in sector_classes for token in tokens):
            cards.append(card)

    results = []
    for card in cards:
        ref_el = card.select_one("p strong span span")
        link_el = card.select_one("a.tcb-post-read-more-link")
        title_el = card.select_one("h2 span")

        if not ref_el or not link_el:
            continue

        results.append(
            {
                "ref": ref_el.get_text(strip=True),
                "url": link_el["href"],
                "title": title_el.get_text(strip=True) if title_el else None,
            }
        )

    return results


def sector_to_slug(sector_label: str) -> str:
    return (
        sector_label.lower()
        .replace("&", "")
        .replace(",", "")
        .replace("  ", " ")
        .replace(" ", "-")
    )


def import_hilton_smythe():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for sector_label, mapping in HILTON_SMYTHE_SECTOR_MAP.items():
            sector_slug = sector_to_slug(sector_label)

            # ✅ ONLY FIX: proper URL encoding
            sector_url = BASE_URL + "?business-sector=" + quote_plus(sector_label)

            print(f"\n=== Hilton Smythe | Sector: {sector_label} ===")
            print(f"[HS] URL: {sector_url}")

            seen_refs: set[str] = set()

            context = browser.new_context()
            page = context.new_page()

            page.goto(sector_url, timeout=30_000)

            # Some sectors legitimately have zero listings
            try:
                page.wait_for_selector(
                    "article.business-listing",
                    state="attached",
                    timeout=15_000,
                )
            except Exception:
                print("[HS] No listings found. Skipping.")
                context.close()
                continue

            page_no = 1

            while True:
                html = page.content()
                listings = parse_sector_listings(html, sector_label)

                new = [l for l in listings if l["ref"] not in seen_refs]

                print(f"[HS] Page {page_no} → {len(new)} listings")

                for item in new:
                    seen_refs.add(item["ref"])

                    if DRY_RUN:
                        print(
                            "DRY_RUN →",
                            item["ref"],
                            mapping["industry"],
                            mapping["sector"],
                            item["url"],
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO deals (
                                source,
                                source_listing_id,
                                source_url,
                                title,
                                industry,
                                sector,
                                sector_source,
                                sector_inference_confidence,
                                sector_inference_reason,
                                needs_detail_refresh,
                                first_seen,
                                last_seen
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1,
                                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT(source, source_listing_id)
                            DO UPDATE SET last_seen = CURRENT_TIMESTAMP
                            """,
                            (
                                BROKER,
                                item["ref"],
                                item["url"],
                                item["title"],
                                mapping["industry"],
                                mapping["sector"],
                                "broker",
                                mapping["confidence"],
                                mapping["reason"],
                            ),
                        )

                # pagination: follow sibling of current page
                current = page.locator(".tcb-pagination-current")
                if not current.count():
                    break

                next_btn = current.locator("xpath=following-sibling::*[1]")
                if not next_btn.count():
                    break

                # JS click to bypass Thrive / HubSpot overlay
                page.evaluate(
                    "(el) => el.click()",
                    next_btn.element_handle(),
                )

                # wait until pagination-current changes
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

                # hard safety cap
                if page_no > 30:
                    break

            if not DRY_RUN:
                conn.commit()

            print(
                f"[HS] Completed sector {sector_label} → "
                f"{len(seen_refs)} listings"
            )

            context.close()

        browser.close()

    conn.close()
    print("\n🏁 Hilton Smythe import complete")


if __name__ == "__main__":
    import_hilton_smythe()