import subprocess
import sys
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "src" / "scripts"

SCRIPTS = [
    # Phase 0
    # "import_legacy_deals.py",
    # "import_dmitry_deals.py",
    #
    # # Phase 1
    # "import_axispartnership.py",
    # "import_businessbuyers.py",
    # "import_businesses4sale.py",
    # "import_dealopportunities.py",
    # "import_knightsbridge.py",
    # "import_hiltonsmythe.py",
    # "import_transworld.py",

    # Phase 2
    "enrich_axispartnership.py",
    "enrich_businessbuyers.py",
    "enrich_businesses4sale.py",
    "enrich_dealopportunities.py",
    "enrich_knightsbridge.py",
    "enrich_hiltonsmythe.py",
    "enrich_transworld.py",

    # Phase 3
    "infer_sectors.py",
    "enrich_financials_from_description.py",
    "recalculate_financial_metrics.py",

    # Phase 4
    "sync_to_sheets.py",
]
def run_script(script_name: str, env=None):
    script_path = SCRIPTS_DIR / script_name
    print(f"\n🚀 Running {script_name}")
    try:
        subprocess.check_call(
            [sys.executable, str(script_path)],
            env={**os.environ, **(env or {})},
        )
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Script failed but pipeline continues: {script_name}")
    print(f"✅ Finished {script_name}")

def main():
    for script in SCRIPTS:
        run_script(script)

    print("\n✅ Full pipeline completed successfully")

if __name__ == "__main__":
    import os
    os.environ["PLAYWRIGHT_HEADLESS"] = "1" # Silent run
    main()