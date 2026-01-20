import subprocess
import sys
from datetime import datetime

BROKERS = [
    {
        "name": "BusinessesForSale",
        "index": "src/scripts/index_businesses4sale.py",
        "enrich": "src/scripts/enrich_businesses4sale_vault.py",
    },
    {
        "name": "BusinessBuyers",
        "index": "src/scripts/index_businessbuyers.py",
        "enrich": "src/scripts/enrich_businessbuyers.py",
    },
    {
        "name": "AxisPartnership",
        "index": None,
        "enrich": "src/scripts/enrich_axispartnership.py",
    },
]

SHEET_SYNC = "src/scripts/sync_google_sheet.py"
PYTHON = sys.executable

def run(cmd: list[str], label: str):
    print(f"\n▶ {label}")
    start = datetime.utcnow()
    res = subprocess.run(cmd)
    if res.returncode != 0:
        raise RuntimeError(f"{label} failed")
    print(f"✔ {label} ({(datetime.utcnow() - start).seconds}s)")

def main():
    print("🚀 Starting full broker refresh")

    for broker in BROKERS:
        name = broker["name"]

        if broker["index"]:
            run(
                [PYTHON, broker["index"]],
                f"{name} — index refresh",
            )

        run(
            [PYTHON, broker["enrich"]],
            f"{name} — enrichment refresh",
        )

    run(
        [PYTHON, SHEET_SYNC],
        "Google Sheet sync",
    )

    print("\n✅ Full refresh complete")