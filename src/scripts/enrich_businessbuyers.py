# src/scripts/enrich_businesses4sale.py

from pathlib import Path
import time
import random
import hashlib

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError

from src.persistence.repository import SQLiteRepository
from src.enrichment.financial_extractor import extract_financial_metrics

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import (
    find_or_create_deal_folder,
    upload_pdf_to_drive,
)
from src.persistence.deal_artifacts import record_deal_artifact

# -----------------------------------
# CONFIG
# -----------------------------------

DRY_RUN = False         # 🔒 flip to False when confident
MAX_DEALS = 100
SLEEP_BETWEEN = (3, 6)

DETAIL_WAIT_SELECTOR = "#hero, div.teaser-content"

PDF_ROOT = Path("/tmp/businesses4sale_pdfs")
PDF_ROOT.mkdir(parents=True, exist_ok=True)

# -----------------------------------
# HELPERS
# -----------------------------------

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None


def extract_teaser_field(soup, heading: str) -> str | None:
    """
    Extracts text from a <div class="teaser-field"> by <h3> title
    """
    for field in soup.select("div.teaser-field"):
        h3 = field.select_one("h3")
        if h3 and h3.get_text(strip=True).lower() == heading.lower():
            return text_or_none(field)
    return None


def compute_content_hash(*parts: str | None) -> str:
    h = hashlib.sha256()
    for p in parts:
        if p:
            h.update(p.encode("utf-8"))
    return h.hexdigest()


# -----------------------------------
# MAIN
# -----------------------------------

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))
    conn = repo.get_conn()
    deals = repo.fetch_deals_needing_details_for_source(
        source="BusinessesForSale",
        limit=MAX_DEALS,
    )

    print(f"🔍 Deals to enrich: {len(deals)}")

    enriched = 0

    for i, deal in enumerate(deals, start=1):
        url = deal["source_url"]
        slug = deal["source_listing_id"]

        print(f"\n➡️ [{i}/{len(deals)}] {slug}")
        print(f"🌐 {url}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            try:
                page.goto(url, timeout=60_000)
                page.wait_for_selector(DETAIL_WAIT_SELECTOR, timeout=20_000)
            except TimeoutError:
                print("⚠️ Detail selector not found — skipping")
                browser.close()
                continue

            # -------------------------------------------------
            # ✅ PDF CAPTURE (NO SCRAPING LOGIC TOUCHED)
            # -------------------------------------------------

            pdf_path = PDF_ROOT / f"{slug}.pdf"
            page.emulate_media(media="screen")
            page.add_style_tag(content="""
            header,
            footer,
            nav,
            .cookie-banner,
            #onetrust-consent-sdk,
            .back-link,
            .request-info,
            .cta,
            button {
                display: none !important;
            }
            """)
            page.pdf(
                path=str(pdf_path),
                format="A4",
                margin={
                    "top": "20mm",
                    "bottom": "20mm",
                    "left": "15mm",
                    "right": "15mm",
                },
                print_background=True,
            )

            if pdf_path.exists():
                size_kb = pdf_path.stat().st_size / 1024
                print(f"🧾 PDF generated: {pdf_path} ({size_kb:.1f} KB)")
            else:
                print("⚠️ PDF generation failed")

            soup = BeautifulSoup(page.content(), "html.parser")

            # -----------------------------------
            # HERO
            # -----------------------------------

            title = text_or_none(soup.select_one("#hero h1"))
            location = text_or_none(soup.select_one("#hero p.location"))
            hero_desc = text_or_none(soup.select_one("#hero p"))

            # -----------------------------------
            # DESCRIPTION
            # -----------------------------------

            business_desc = extract_teaser_field(soup, "Business Description")
            description = business_desc or hero_desc

            # -----------------------------------
            # FINANCIALS (UK + INTL)
            # -----------------------------------

            financial_chunks: list[str] = []

            # (1) UK inline financials
            for dl in soup.select("div.financials dl"):
                dt = dl.select_one("dt")
                dd = dl.select_one("dd")
                if dt and dd:
                    financial_chunks.append(
                        f"{dt.get_text(strip=True)} {dd.get_text(strip=True)}"
                    )

            # (2) Narrative financial summary
            financial_summary = extract_teaser_field(soup, "Financial Summary")
            if financial_summary:
                financial_chunks.append(financial_summary)

            financial_text = " ".join(financial_chunks).strip() or None

            extracted = extract_financial_metrics(financial_text or "")

            # -----------------------------------
            # CONTENT HASH
            # -----------------------------------

            content_hash = compute_content_hash(
                title,
                location,
                description,
                financial_text,
            )

            # -----------------------------------
            # UPDATES
            # -----------------------------------

            updates = {}

            if title:
                updates["title"] = title

            if location:
                updates["location"] = location

            if description:
                updates["description"] = description

            updates["content_hash"] = content_hash
            updates["needs_detail_refresh"] = 0

            for field in ("revenue_k", "ebitda_k", "asking_price_k"):
                val = extracted.get(field)
                if val and deal.get(field) is None:
                    updates[field] = val["value"]

            if updates:
                print("✍️ UPDATES:", updates)

                if not DRY_RUN:
                    parent_folder_id = get_drive_parent_folder_id(
                        industry=deal.get("industry") or "Other",
                        broker="BusinessesForSale",
                    )

                    deal_folder_id = find_or_create_deal_folder(
                        parent_folder_id=parent_folder_id,
                        deal_id=f"BFS-{slug}",
                        deal_title=title,
                    )

                    pdf_drive_url = upload_pdf_to_drive(
                        local_path=str(pdf_path),
                        filename=f"{slug}.pdf",
                        folder_id=deal_folder_id,
                    )

                    drive_file_id = pdf_drive_url.split("/d/")[1].split("/")[0]

                    record_deal_artifact(
                        conn=conn,
                        deal_id=deal["id"],
                        broker="BusinessesForSale",
                        artifact_type="pdf",
                        artifact_name=f"{slug}.pdf",
                        drive_file_id=drive_file_id,
                        drive_url=pdf_drive_url,
                        industry=deal.get("industry"),
                        sector=deal.get("sector"),
                        created_by="enrich_businesses4sale.py",
                    )
                    conn.commit()
                    time.sleep(0.1)
                    updates["pdf_drive_url"] = pdf_drive_url
                    updates["drive_folder_url"] = (
                            "https://drive.google.com/drive/folders/" + deal_folder_id
                    )
                    repo.update_detail_fields_by_source(
                        source=deal["source"],
                        source_listing_id=deal["source_listing_id"],
                        fields=updates,
                    )
                    enriched += 1

            browser.close()
            time.sleep(random.uniform(*SLEEP_BETWEEN))

    print(f"\n✅ BusinessesForSale enrichment complete — updated {enriched} deals")


if __name__ == "__main__":
    main()