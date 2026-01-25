import os
from pathlib import Path
from src.utils.run_scripts import run_script

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# -------------------------------------
# Phase 1 — Data computation scripts
# -------------------------------------
DATA_SCRIPTS = [
    "infer_sectors.py",
    "enrich_financials_from_description.py",
    "recalculate_financial_metrics.py",
]

# -------------------------------------
# Helpers
# -------------------------------------
def run_sync_phase(phase: str):
    """
    Runs sync_to_sheets.py in the given phase (DATA or FORMAT)
    """
    if phase not in {"DATA", "FORMAT"}:
        raise ValueError(f"Invalid SHEETS_PHASE: {phase}")

    os.environ["SHEETS_PHASE"] = phase
    elapsed, status = run_script("sync_to_sheets.py")

    return (f"sync_to_sheets[{phase}]", elapsed, status)


# -------------------------------------
# Main pipeline
# -------------------------------------
def main():
    timings = []

    print("\n▶ DATA COMPUTATION PHASE")
    for script in DATA_SCRIPTS:
        elapsed, status = run_script(script)
        timings.append((script, elapsed, status))

    print("\n▶ SHEETS DATA SYNC PHASE")
    timings.append(run_sync_phase("DATA"))

    print("\n▶ SHEETS FORMAT SYNC PHASE")
    timings.append(run_sync_phase("FORMAT"))

    # ---------------------------------
    # Timing summary
    # ---------------------------------
    print("\n📊 PIPELINE TIMING SUMMARY")
    for script, elapsed, status in sorted(timings, key=lambda x: x[1], reverse=True):
        print(f"{elapsed:7.1f}s  {status:6}  {script}")

    total = sum(t for _, t, _ in timings)
    print(f"\n⏱️ TOTAL PIPELINE TIME: {total/60:.1f} minutes")


# -------------------------------------
# Entrypoint
# -------------------------------------
if __name__ == "__main__":
    os.environ["PLAYWRIGHT_HEADLESS"] = "1"
    main()