import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
OUTPUT_DIR = Path("/tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

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
    "Unassessed",
    "Pass",
    "Initial Contact",
    "CIM",
    "CIM DD",
    "Meeting",
    "LOI",
    "Lost"
]

# -------------------------------------------------
# STATUS NORMALIZATION (CRITICAL)
# -------------------------------------------------

PIPELINE_STATUS_CANONICAL = {
    "Unassessed":"Unassessed",
    "Pass":"Pass",
    "Initial Contact":"Initial Contact",
    "CIM":"CIM",
    "CIM DD":"CIM DD",
    "Meeting":"Meeting",
    "LOI":"LOI",
    "Lost":"Lost"
}


def normalize_status(raw):
    if raw is None:
        return "not_yet_analysed"
    return PIPELINE_STATUS_CANONICAL.get(raw, raw.strip().lower())

from datetime import date

def current_snapshot_key():
    iso_year, iso_week, _ = date.today().isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

# -------------------------------------------------
# MAIN PLOT FUNCTION
# -------------------------------------------------

def plot_latest_pipeline_snapshot(
        force_current_week: bool = False
):
    conn = sqlite3.connect(DB_PATH)
    snapshot_key = (
        current_snapshot_key()
        if force_current_week
        else None
    )

    query = """
            SELECT snapshot_key, \
                   COALESCE(industry, 'NA') AS industry, \
                   status, \
                   deal_count
            FROM pipeline_snapshots \
            """

    if snapshot_key:
        query += " WHERE snapshot_key = ?"
        params = (snapshot_key,)
    else:
        query += " WHERE snapshot_key = (SELECT MAX(snapshot_key) FROM pipeline_snapshots)"
        params = ()

    snapshot = pd.read_sql(query, conn, params=params)

    conn.close()

    if snapshot.empty:
        raise RuntimeError("No snapshot data found")

    snapshot_key = snapshot["snapshot_key"].iloc[0]

    # -----------------------------
    # Normalize status
    # -----------------------------
    snapshot["status"] = snapshot["status"].apply(normalize_status)

    # -----------------------------
    # Enforce industry ordering
    # -----------------------------
    snapshot["industry"] = pd.Categorical(
        snapshot["industry"],
        categories=INDUSTRY_ORDER,
        ordered=True,
    )

    snapshot = snapshot.sort_values("industry")

    # -----------------------------
    # Pivot
    # -----------------------------
    pivot = snapshot.pivot_table(
        index="industry",
        columns="status",
        values="deal_count",
        aggfunc="sum",
        fill_value=0,
        observed=False,
    )

    # -----------------------------
    # Enforce status order (THIS FIXES DATA LOSS)
    # -----------------------------
    pivot = pivot.reindex(
        columns=[s for s in PIPELINE_STATUS_ORDER if s in pivot.columns],
        fill_value=0,
    )

    # -----------------------------
    # Plot
    # -----------------------------
    fig, ax = plt.subplots(figsize=(18, 8))

    pivot.plot(
        kind="bar",
        stacked=True,
        ax=ax,
    )

    ax.set_title(f"Pipeline Snapshot — {snapshot_key}", fontsize=14)
    ax.set_xlabel("Industry")
    ax.set_ylabel("Deals")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right")

    # -----------------------------
    # Legend ordering (EXPLICIT)
    # -----------------------------
    handles, labels = ax.get_legend_handles_labels()
    label_to_handle = dict(zip(labels, handles))

    ordered_labels = [
        s for s in PIPELINE_STATUS_ORDER if s in label_to_handle
    ]

    ax.legend(
        [label_to_handle[l] for l in ordered_labels],
        ordered_labels,
        title="Status",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
    )

    plt.tight_layout()

    output_path = OUTPUT_DIR / f"pipeline_snapshot_{snapshot_key}.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()

    return snapshot_key, output_path


# -------------------------------------------------
# ENTRYPOINT (FOR LOCAL TESTING)
# -------------------------------------------------

if __name__ == "__main__":
    key, path = plot_latest_pipeline_snapshot()
    print(f"✅ Chart generated: {path}")