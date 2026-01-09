from src.scripts.snapshot_weekly_pipeline import snapshot_pipeline_run
from src.scripts.snapshot_weekly_chart import plot_latest_pipeline_snapshot
from src.scripts.snapshot_weekly_slack import post_snapshot_to_slack
import os

FORCE_CURRENT_WEEK = os.getenv("FORCE_CURRENT_WEEK", "0") == "1"

def main(force_current_week: bool = False):
    # 1️⃣ Create / overwrite snapshot (ONLY place force applies)
    snapshot_pipeline_run(force_current_week=FORCE_CURRENT_WEEK)

    # 2️⃣ Read latest snapshot + chart (read-only)
    snapshot_key, chart_path = plot_latest_pipeline_snapshot()

    # 3️⃣ Publish
    post_snapshot_to_slack(snapshot_key, chart_path)

    print(f"✅ Snapshot {snapshot_key} charted and sent to Slack")


if __name__ == "__main__":
    main()