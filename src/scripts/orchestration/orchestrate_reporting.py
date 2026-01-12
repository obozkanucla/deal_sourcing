from pathlib import Path
import os

from src.reporting.pipeline_charts import PipelineCharts
from src.reporting.slack_reporter import SlackReporter
from src.reporting.snapshot_builder import snapshot_pipeline_run

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[3] / "db" / "deals.sqlite"

TMP_DIR = Path("/tmp")
TMP_DIR.mkdir(exist_ok=True)

FORCE_CURRENT_WEEK = os.getenv("FORCE_CURRENT_WEEK", "0") == "1"


# --------------------------------------------------
# ORCHESTRATOR
# --------------------------------------------------

def main() -> None:
    """
    Weekly pipeline reporting orchestrator.

    Responsibilities:
    - ensure snapshot exists
    - generate charts (3)
    - compose report message
    - deliver via Slack
    """

    # --------------------------------------------------
    # 1. Build / refresh snapshot (mutation)
    # --------------------------------------------------
    snapshot_key = snapshot_pipeline_run(
        force_current_week=FORCE_CURRENT_WEEK
    )

    # --------------------------------------------------
    # 2. Generate charts (read-only)
    # --------------------------------------------------
    charts = PipelineCharts(DB_PATH)

    chart_paths = []

    # Chart 1 — Industry × Status snapshot
    _, _, snap_key = charts.plot_snapshot_by_industry(
        snapshot_key=snapshot_key,
        output_path=TMP_DIR / "pipeline_by_industry.png",
    )
    chart_paths.append(TMP_DIR / "pipeline_by_industry.png")

    # Chart 2 — Weekly funnel delta
    _, _, week_prev, week_curr = charts.plot_weekly_funnel_delta(
        output_path=TMP_DIR / "weekly_funnel_delta.png",
    )
    chart_paths.append(TMP_DIR / "weekly_funnel_delta.png")

    # Chart 3 — Funnel stock (centered)
    _, _, _ = charts.plot_funnel_stock(
        snapshot_key=snapshot_key,
        output_path=TMP_DIR / "funnel_stock.png",
    )
    chart_paths.append(TMP_DIR / "funnel_stock.png")

    # --------------------------------------------------
    # 3. Deliver to Slack (transport only)
    # --------------------------------------------------
    header = (
        f"📊 *Weekly Pipeline Report — {snapshot_key}*\n\n"
        "• Funnel stock\n"
        "• Weekly funnel movement\n"
        "• Industry × status breakdown\n"
        "• Generated automatically"
    )

    slack = SlackReporter()
    slack.post_message(header)
    slack.upload_files(chart_paths)


# --------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------

if __name__ == "__main__":
    main()