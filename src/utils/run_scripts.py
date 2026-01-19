import time
import subprocess
import sys
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "src" / "scripts"


def run_script(script_name: str, env=None):
    script_path = SCRIPTS_DIR / script_name
    print(f"\n🚀 Running {script_name}")

    start = time.perf_counter()

    env_vars = {
        **os.environ,
        "PYTHONPATH": str(PROJECT_ROOT),
        **(env or {}),
    }

    try:
        subprocess.check_call(
            [sys.executable, str(script_path)],
            env=env_vars,
        )
        status = "ok"
    except subprocess.CalledProcessError:
        print(f"⚠️ Script failed but pipeline continues: {script_name}")
        status = "failed"

    elapsed = time.perf_counter() - start
    print(f"⏱️ {script_name} finished in {elapsed:.1f}s ({status})")

    return elapsed, status