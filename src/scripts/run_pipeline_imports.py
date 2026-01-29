import time
import os
import json
from pathlib import Path

from src.utils.run_scripts import run_script

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SCRIPTS = [
    # "import_abercorn.py",
    # "import_axispartnership.py",
    # "import_bsr.py",
    # "import_businessbuyers.py",
    # "import_businesses4sale_vault.py",
    # "import_businesses4sale_generic.py",
    "import_daltons.py",
    "import_dealopportunities.py",
    "import_dmitry_deals.py",
    "import_hiltonsmythe.py",
    "import_knightsbridge.py",
    "import_legacy_deals.py",
    "import_transworld.py",
]

FAILURE_REPORT_PATH = PROJECT_ROOT / "pipeline_failures.json"


def main():
    timings = []
    failed_scripts = []

    for script in SCRIPTS:
        elapsed, status = run_script(script)
        timings.append((script, elapsed, status))

        if status != "ok":
            failed_scripts.append(script)

    # -----------------------------
    # Timing summary (unchanged)
    # -----------------------------
    print("\n📊 PIPELINE TIMING SUMMARY")
    for script, elapsed, status in sorted(
        timings, key=lambda x: x[1], reverse=True
    ):
        print(f"{elapsed:7.1f}s  {status:6}  {script}")

    total = sum(t for _, t, _ in timings)
    print(f"\n⏱️ TOTAL PIPELINE TIME: {total/60:.1f} minutes")

    # -----------------------------
    # Failure signal for GitHub Actions
    # -----------------------------
    if failed_scripts:
        with open(FAILURE_REPORT_PATH, "w") as f:
            json.dump(failed_scripts, f)

        print(
            f"\n⚠️ {len(failed_scripts)} script(s) failed. "
            f"Details written to {FAILURE_REPORT_PATH}"
        )
    else:
        # Ensure stale file never survives
        if FAILURE_REPORT_PATH.exists():
            FAILURE_REPORT_PATH.unlink()


if __name__ == "__main__":
    os.environ["PLAYWRIGHT_HEADLESS"] = "1"
    main()