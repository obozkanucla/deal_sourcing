from dotenv import load_dotenv
load_dotenv()

import os
from pathlib import Path

# ---------- Credentials ----------
BB_USERNAME = os.getenv("BB_USERNAME")
BB_PASSWORD = os.getenv("BB_PASSWORD")

if not BB_USERNAME or not BB_PASSWORD:
    raise RuntimeError("BB_USERNAME or BB_PASSWORD not set")

# ---------- Rate limiting ----------
DAILY_DETAIL_PAGE_BUDGET = 25

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
PDF_DIR = DATA_DIR / "pdf_snapshots"

DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)