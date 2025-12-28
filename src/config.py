# src/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# BusinessBuyers
BB_USERNAME = os.getenv("BB_USERNAME")
BB_PASSWORD = os.getenv("BB_PASSWORD")
KB_USERNAME = os.getenv("KB_USERNAME")
KB_PASSWORD = os.getenv("KB_PASSWORD")

if not BB_USERNAME or not BB_PASSWORD:
    raise RuntimeError(
        "Missing BusinessBuyers credentials. "
        "Set BB_USERNAME and BB_PASSWORD in .env"
    )