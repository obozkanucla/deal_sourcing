from pathlib import Path
from playwright.sync_api import sync_playwright


def html_to_pdf(html: str, output_path: Path):
    """
    Render HTML to PDF using Playwright (Chromium).
    No wkhtmltopdf required.
    """
    output_path = Path(output_path)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Load HTML directly
        page.set_content(html, wait_until="networkidle")

        # Save PDF
        page.pdf(
            path=str(output_path),
            format="A4",
            print_background=True,
            margin={
                "top": "20mm",
                "bottom": "20mm",
                "left": "15mm",
                "right": "15mm",
            },
        )

        browser.close()