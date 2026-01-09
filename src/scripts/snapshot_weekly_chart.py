import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
SNAPSHOT_KEY = "2026-W02"   # or compute dynamically
INDUSTRY_ORDER = [
    "Healthcare",
    "Business_Services",
    "Technology",
    "Industrials",
    "Construction_Built_Environment",
    "Consumer_Retail",
    "Food_Beverage",
    "Logistics_Distribution",
    "Education",
    "Financial_Services",
    "Other",
    "NA",  # always last
]

PIPELINE_STATUS_ORDER = [
    "new",
    "not_yet_analysed",
    "initial_contact",
    "cim",
    "cim_dd",
    "1st_meeting_(online)",
    "1st_meeting_(in_person)",
    "2nd_meeting_(online)",
    "2nd_meeting_(in_person)",
    "loi",
    "pass",
    "lost",
]

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
OUTPUT_DIR = Path("/tmp")
OUTPUT_DIR.mkdir(exist_ok=True)
import matplotlib
import sys

def plot_latest_pipeline_snapshot(show=False):
    conn = sqlite3.connect(DB_PATH)

    snapshot = pd.read_sql(
        """
        SELECT *
        FROM pipeline_snapshots
        WHERE snapshot_key = (
            SELECT MAX(snapshot_key) FROM pipeline_snapshots
        )
        """,
        conn,
    )
    conn.close()

    if snapshot.empty:
        raise RuntimeError("No snapshot data found")

    snapshot_key = snapshot["snapshot_key"].iloc[0]

    snapshot["industry"] = pd.Categorical(
        snapshot["industry"],
        categories=INDUSTRY_ORDER,
        ordered=True,
    )

    snapshot = snapshot.sort_values("industry")

    pivot = snapshot.pivot_table(
        index="industry",
        columns="status",
        values="deal_count",
        aggfunc="sum",
        fill_value=0,
    )

    # ---- EXPLICIT FIGURE + AXIS ----
    fig, ax = plt.subplots(figsize=(16, 7))

    pivot.plot(
        kind="bar",
        stacked=True,
        ax=ax,
    )

    ax.set_title(f"Pipeline Snapshot — {snapshot_key}")
    ax.set_xlabel("Industry")
    ax.set_ylabel("Deals")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right")

    ax.legend(
        title="Status",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
    )

    output_path = OUTPUT_DIR / f"pipeline_snapshot_{snapshot_key}.png"
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")

    # ---- ONLY SHOW IF INTERACTIVE ----
    if show and matplotlib.get_backend().lower() != "agg":
        plt.show()

    plt.close(fig)
    print(snapshot_key)
    return snapshot_key, output_path

if __name__ == "__main__":
    plot_latest_pipeline_snapshot(show=True)