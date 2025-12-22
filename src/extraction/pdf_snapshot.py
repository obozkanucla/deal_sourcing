from datetime import date
from pathlib import Path


def save_pdf(page, source_listing_id, base_dir):
    today = date.today().isoformat()
    out_dir = Path(base_dir) / today
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = out_dir / f"BB_{source_listing_id}.pdf"
    page.pdf(path=str(pdf_path))

    return str(pdf_path)