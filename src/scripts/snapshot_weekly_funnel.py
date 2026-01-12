import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

FUNNEL_ORDER = [
    "Unassessed",
    "Initial Contact",
    "CIM",
    "CIM DD",
    "Meeting",
    "LOI",
    "Pass",
    "Lost",
]

STAGE_COLORS = {
    "Unassessed": "#e5f5e0",
    "Initial Contact": "#c7e9c0",
    "CIM": "#a1d99b",
    "CIM DD": "#74c476",
    "Meeting": "#41ab5d",
    "LOI": "#238b45",
    "Pass": "#d62728",
    "Lost": "#8c1d18",
}

# -------------------------------------------------
# GET LATEST SNAPSHOT WEEK
# -------------------------------------------------

def get_latest_snapshot_key(db_path):
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT MAX(snapshot_key) FROM pipeline_snapshots"
    ).fetchone()
    conn.close()

    if not row or not row[0]:
        raise RuntimeError("No snapshots found")

    return row[0]


WEEK_CURRENT = get_latest_snapshot_key(DB_PATH)

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------

query = """
SELECT
    status,
    SUM(deal_count) AS deals
FROM pipeline_snapshots
WHERE snapshot_key = ?
GROUP BY status;
"""

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql(query, conn, params=(WEEK_CURRENT,))
conn.close()

df["status"] = pd.Categorical(
    df["status"],
    categories=FUNNEL_ORDER,
    ordered=True,
)
df = df.sort_values("status")

df["color"] = df["status"].map(STAGE_COLORS)

# -------------------------------------------------
# CENTERED FUNNEL PLOT
# -------------------------------------------------

fig, ax = plt.subplots(figsize=(8, 6))

max_val = df["deals"].max()
lefts = -df["deals"] / 2

bars = ax.barh(
    df["status"],
    df["deals"],
    left=lefts,
    color=df["color"],
)

# Hide x-axis entirely
ax.set_xticks([])
ax.set_xlabel("")
ax.set_ylabel("")

ax.set_xlim(-max_val * 0.55, max_val * 0.55)

ax.set_title(f"Funnel Stock — {WEEK_CURRENT}")

# Annotate totals (centered on bars)
for bar, value in zip(bars, df["deals"]):
    ax.text(
        0,
        bar.get_y() + bar.get_height() / 2,
        f"{int(value)}",
        va="center",
        ha="center",
        fontsize=10,
        color="black",
        fontweight="bold",
    )

# Clean look
for spine in ["top", "right", "bottom", "left"]:
    ax.spines[spine].set_visible(False)

plt.tight_layout()
plt.show()