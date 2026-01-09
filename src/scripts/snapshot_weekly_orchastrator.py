from src.scripts.snapshot_weekly_pipeline import get_snapshot_week
from src.scripts.snapshot_weekly_chart import plot_latest_pipeline_snapshot
from src.scripts.snapshot_weekly_slack import post_snapshot_to_slack

def main():
    snapshot_key, chart_path = plot_latest_pipeline_snapshot()
    post_snapshot_to_slack(snapshot_key, chart_path)
    print(f"✅ Snapshot {snapshot_key} charted and sent to Slack")

if __name__ == "__main__":
    main()