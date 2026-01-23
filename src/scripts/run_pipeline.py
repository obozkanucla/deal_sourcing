import time
import subprocess
import sys
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "src" / "scripts"

SCRIPTS = [
    "import_axispartnership.py",
    "import_businessbuyers.py",
    "import_businesses4sale_vault.py",
    "import_dealopportunities.py",
    "import_knightsbridge.py",
    "import_hiltonsmythe.py",
    "import_transworld.py",
    "import_abercorn.py",
    "import_businesses4sale_generic.py"

    "enrich_axispartnership.py",
    "enrich_businessbuyers.py",
    "enrich_businesses4sale_vault.py",
    "enrich_dealopportunities.py",
    "enrich_knightsbridge.py",
    "enrich_hiltonsmythe.py",
    "enrich_transworld.py",
    "enrich_abercorn.py",

    "infer_sectors.py",
    "enrich_financials_from_description.py",
    "recalculate_financial_metrics.py",
    "sync_to_sheets.py",
]

def run_script(script_name: str, env=None):
    script_path = SCRIPTS_DIR / script_name
    print(f"\n🚀 Running {script_name}")

    start = time.perf_counter()

    try:
        subprocess.check_call(
            [sys.executable, str(script_path)],
            env={**os.environ, **(env or {})},
        )
        status = "ok"
    except subprocess.CalledProcessError:
        print(f"⚠️ Script failed but pipeline continues: {script_name}")
        status = "failed"

    elapsed = time.perf_counter() - start
    print(f"⏱️ {script_name} finished in {elapsed:.1f}s ({status})")

    return elapsed, status


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