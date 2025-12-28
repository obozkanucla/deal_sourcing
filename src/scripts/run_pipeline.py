import subprocess
import sys
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "src" / "scripts"

def run_script(name: str, script_name: str, env=None):
    script_path = SCRIPTS_DIR / script_name
    print(f"\n🚀 Running {name}")
    subprocess.check_call(
        [sys.executable, str(script_path)],
        env={**os.environ, **(env or {})},
    )
    print(f"✅ Finished {name}")

def main():
    # run_script("Legacy Deals Import", "import_legacy_deals.py")
    # run_script("Dmitry Deals Import", "import_dmitry_deals.py")
    # run_script("BusinessBuyers Index", "import_businessbuyers.py")
    # run_script("DealOpportunities Index", "import_dealopportunities.py")
    run_script(
        "BusinessBuyers Detail",
        "enrich_businessbuyers.py",
        env={"ENRICH_LIMIT": "none"},
    )
    run_script(
        "DealOpportunities Detail",
        "enrich_dealopportunities.py",
        env={"ENRICH_LIMIT": "none"},
    )
    run_script("Sector Inference", "infer_sectors.py")

if __name__ == "__main__":
    main()