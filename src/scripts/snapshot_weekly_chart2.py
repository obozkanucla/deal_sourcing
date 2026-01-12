import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

FUNNEL_ORDER = [
    "New",
    "Unassessed",
    "Initial Contact",
    "CIM",
    "CIM DD",
    "Meeting",
    "LOI",
    "Pass",
    "Lost",
]

# -------------------------------------------------
# GET LATEST TWO SNAPSHOT KEYS
# -------------------------------------------------
def get_latest_two_snapshot_keys(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT snapshot_key
        FROM pipeline_snapshots
        ORDER BY snapshot_key DESC
        LIMIT 2;
        """
    ).fetchall()
    conn.close()

    if len(rows) < 2:
        raise RuntimeError("Not enough snapshots to compute weekly delta")

    return rows[1][0], rows[0][0]  # prev, curr
# -------------------------------------------------
# QUERY
# -------------------------------------------------

WEEK_PREV, WEEK_CURR = get_latest_two_snapshot_keys(DB_PATH)

query = f"""
WITH weekly AS (
    SELECT
        snapshot_key,
        status,
        SUM(deal_count) AS deals
    FROM pipeline_snapshots
    GROUP BY snapshot_key, status
)
SELECT
    status,
    SUM(CASE WHEN snapshot_key = '{WEEK_CURR}' THEN deals ELSE 0 END)
  - SUM(CASE WHEN snapshot_key = '{WEEK_PREV}' THEN deals ELSE 0 END)
    AS delta
FROM weekly
GROUP BY status;
"""

# -------------------------------------------------
# LOAD DATA
# -------------------------------------------------

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql(query, conn)
conn.close()

# enforce funnel ordering
df["status"] = pd.Categorical(
    df["status"],
    categories=FUNNEL_ORDER,
    ordered=True,
)

df = df.sort_values("status")

# -------------------------------------------------
# COLOR LOGIC (directional, not semantic)
# -------------------------------------------------

df["color"] = df["delta"].apply(
    lambda x: "#2ca02c" if x > 0 else "#d62728"
)

# -------------------------------------------------
# PLOT
# -------------------------------------------------

fig, ax = plt.subplots(figsize=(10, 5))

ax.barh(
    df["status"],
    df["delta"],
    color=df["color"],
)

ax.axvline(0, linewidth=1)

ax.set_title(f"Weekly Funnel Stock Change ({WEEK_CURR} vs {WEEK_PREV})")
ax.set_xlabel("Net change in deal count")
ax.set_ylabel("Funnel stage")

plt.tight_layout()
plt.show()