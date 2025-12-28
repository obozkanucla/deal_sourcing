from src.config import KB_USERNAME, KB_PASSWORD

import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
PDF_ROOT = Path("/tmp/knightsbridge_pdfs")

KNIGHTSBRIDGE_BASE = "https://www.knightsbridgeplc.com"

RESTART_EVERY = 50        # recycle browser session
BASE_SLEEP = 1.2
JITTER = 0.8

LOGIN_EMAIL = KB_USERNAME
LOGIN_PASSWORD = KB_PASSWORD

if not LOGIN_EMAIL or not LOGIN_PASSWORD:
    raise RuntimeError("KNIGHTSBRIDGE_EMAIL / KNIGHTSBRIDGE_PASSWORD not set in .env")

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def _sleep(extra: float = 0.0):
    time.sleep(BASE_SLEEP + extra + (JITTER * os.urandom(1)[0] / 255))


def _extract_description(page) -> Optional[str]:
    """
    Extracts the main overview paragraph(s) from BusinessDetails.
    """
    try:
        blocks = page.locator("#BusinessDetails p")
        texts = []

        for i in range(blocks.count()):
            t = blocks.nth(i).inner_text().strip()
            if len(t) > 40:  # filter boilerplate
                texts.append(t)

        return "\n\n".join(texts) if texts else None
    except Exception:
        return None


def _extract_price(page) -> Optional[str]:
    try:
        price = page.locator(".btn.price")
        if price.count():
            return price.first.inner_text().strip()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------
# CLIENT
# ---------------------------------------------------------------------

class KnightsbridgeClient:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None

    def start(self):
        print("🚀 Starting Knightsbridge client")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()

    def stop(self):
        print("🛑 Stopping Knightsbridge client")
        try:
            if self.browser:
                self.browser.close()
        finally:
            if self.playwright:
                self.playwright.stop()

    # -----------------------------------------------------------------

    def login(self):
        print("🔐 Logging into Knightsbridge")
        self.page.goto(f"{KNIGHTSBRIDGE_BASE}/login/", wait_until="domcontentloaded")

        self.page.fill("#LoginEmail", LOGIN_EMAIL)
        self.page.fill("#LoginPassword", LOGIN_PASSWORD)

        self.page.evaluate("LoginUser('#ContentPlaceHolder1_ctl09')")
        self.page.wait_for_timeout(3_000)

        if self.page.locator("text=Logout").count() == 0:
            raise RuntimeError("Knightsbridge login failed")

        print("✅ Logged in successfully")


# ---------------------------------------------------------------------
# ENRICH PIPELINE
# ---------------------------------------------------------------------

def enrich_knightsbridge(limit: Optional[int] = None):
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT source_listing_id, source_url
        FROM deals
        WHERE source = 'Knightsbridge'
          AND (
                needs_detail_refresh = 1
                OR detail_fetched_at IS NULL
                OR description IS NULL
          )
        ORDER BY source_listing_id
        """
    ).fetchall()

    if limit:
        rows = rows[:limit]

    print(f"🔍 Found {len(rows)} Knightsbridge deals needing details")

    if not rows:
        print("✅ Nothing to enrich")
        return

    client = KnightsbridgeClient()
    client.start()
    client.login()

    processed = 0

    try:
        for row in rows:
            listing_id = row["source_listing_id"]
            url = row["source_url"]

            # ----------------------------------------------------------
            # Session recycling
            # ----------------------------------------------------------
            if processed > 0 and processed % RESTART_EVERY == 0:
                print("🔁 Recycling browser session")
                client.stop()
                client = KnightsbridgeClient()
                client.start()
                client.login()

            processed += 1

            print(f"\n➡️ Enriching Knightsbridge {listing_id}")
            full_url = url if url.startswith("http") else f"{KNIGHTSBRIDGE_BASE}{url}"

            try:
                client.page.goto(
                    full_url,
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )

                client.page.wait_for_selector(
                    "#BusinessDetails",
                    timeout=15_000,
                )

                _sleep(0.6)

                description = _extract_description(client.page)
                price = _extract_price(client.page)

                fetched_at = datetime.utcnow().isoformat(timespec="seconds")

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        description = ?,
                        asking_price = ?,
                        detail_fetched_at = ?,
                        needs_detail_refresh = 0,
                        detail_fetch_reason = NULL,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE source = 'Knightsbridge'
                      AND source_listing_id = ?
                    """,
                    (
                        description,
                        price,
                        fetched_at,
                        listing_id,
                    ),
                )
                conn.commit()

                print("✅ Enriched")
                print(f"   Description: {'YES' if description else 'NO'}")
                if price:
                    print(f"   Price: {price}")

            except PlaywrightTimeout as e:
                print(f"⚠️ Timeout on {listing_id}")

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        needs_detail_refresh = 1,
                        detail_fetch_reason = ?
                    WHERE source = 'Knightsbridge'
                      AND source_listing_id = ?
                    """,
                    (f"timeout: {e}", listing_id),
                )
                conn.commit()
                continue

            except Exception as e:
                print(f"❌ Error on {listing_id}: {e}")

                conn.execute(
                    """
                    UPDATE deals
                    SET
                        needs_detail_refresh = 1,
                        detail_fetch_reason = ?
                    WHERE source = 'Knightsbridge'
                      AND source_listing_id = ?
                    """,
                    (str(e)[:500], listing_id),
                )
                conn.commit()
                continue

    finally:
        client.stop()
        conn.close()

    print("\n🏁 Knightsbridge detail enrichment complete")


# ---------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    enrich_knightsbridge()