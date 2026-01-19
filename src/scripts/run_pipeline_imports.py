import time
import subprocess
import sys
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "src" / "scripts"
from src.utils.run_scripts import run_script

SCRIPTS = [
    "import_axispartnership.py",
    "import_businessbuyers.py",
    "import_businesses4sale.py",
    "import_dealopportunities.py",
    "import_knightsbridge.py",
    "import_hiltonsmythe.py",
    "import_transworld.py",

    # "enrich_axispartnership.py",
    # "enrich_businessbuyers.py",
    # "enrich_businesses4sale.py",
    # "enrich_dealopportunities.py",
    # "enrich_knightsbridge.py",
    # "enrich_hiltonsmythe.py",
    # "enrich_transworld.py",
    #
    # "infer_sectors.py",
    # "enrich_financials_from_description.py",
    # "recalculate_financial_metrics.py",
    # "sync_to_sheets.py",
]

def main():
    timings = []

    for script in SCRIPTS:
        elapsed, status = run_script(script)
        timings.append((script, elapsed, status))

    print("\n📊 PIPELINE TIMING SUMMARY")
    for script, elapsed, status in sorted(timings, key=lambda x: x[1], reverse=True):
        print(f"{elapsed:7.1f}s  {status:6}  {script}")

    total = sum(t for _, t, _ in timings)
    print(f"\n⏱️ TOTAL PIPELINE TIME: {total/60:.1f} minutes")


if __name__ == "__main__":
    os.environ["PLAYWRIGHT_HEADLESS"] = "1"
    main()