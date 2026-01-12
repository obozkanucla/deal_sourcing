from pathlib import Path
from typing import Optional, Tuple
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

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

STAGE_COLORS = {
    "New": "#edf8e9",
    "Unassessed": "#c7e9c0",
    "Initial Contact": "#a1d99b",
    "CIM": "#74c476",
    "CIM DD": "#41ab5d",
    "Meeting": "#238b45",
    "LOI": "#006d2c",
    "Pass": "#d62728",
    "Lost": "#8c1d18",
}

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
    "NA",
]


# ------------------------------------------------------------------
# PipelineCharts
# ------------------------------------------------------------------

class PipelineCharts:
    """
    Read-only reporting layer over pipeline_snapshots.
    Produces matplotlib figures. Never mutates data.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path

    # --------------------------------------------------------------
    # Snapshot helpers
    # --------------------------------------------------------------

    def latest_snapshot_key(self) -> str:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(snapshot_key) FROM pipeline_snapshots"
            ).fetchone()

        if not row or not row[0]:
            raise RuntimeError("No snapshots found")

        return row[0]

    def latest_two_snapshot_keys(self) -> Tuple[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT snapshot_key
                FROM pipeline_snapshots
                ORDER BY snapshot_key DESC
                LIMIT 2
                """
            ).fetchall()

        if len(rows) < 2:
            raise RuntimeError("Not enough snapshots for weekly delta")

        return rows[1][0], rows[0][0]  # prev, curr

    # --------------------------------------------------------------
    # Chart 1 — Snapshot by Industry × Status
    # --------------------------------------------------------------

    def plot_snapshot_by_industry(
        self,
        snapshot_key: Optional[str] = None,
        output_path: Optional[Path] = None,
        show: bool = False,
    ):
        if snapshot_key is None:
            snapshot_key = self.latest_snapshot_key()

        query = """
        SELECT
            COALESCE(industry, 'NA') AS industry,
            status,
            SUM(deal_count) AS deals
        FROM pipeline_snapshots
        WHERE snapshot_key = ?
        GROUP BY industry, status
        """

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(snapshot_key,))

        df["industry"] = pd.Categorical(
            df["industry"], categories=INDUSTRY_ORDER, ordered=True
        )
        df["status"] = pd.Categorical(
            df["status"], categories=FUNNEL_ORDER, ordered=True
        )

        pivot = df.pivot_table(
            index="industry",
            columns="status",
            values="deals",
            aggfunc="sum",
            fill_value=0,
            observed=False,
        )

        fig, ax = plt.subplots(figsize=(18, 8))
        pivot.plot(kind="bar", stacked=True, ax=ax)

        ax.set_title(f"Pipeline Snapshot — {snapshot_key}")
        ax.set_xlabel("Industry")
        ax.set_ylabel("Deals")
        ax.tick_params(axis="x", rotation=30)

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            title="Status",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            frameon=False,
        )

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=150, bbox_inches="tight")

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig, ax, snapshot_key

    # --------------------------------------------------------------
    # Chart 2 — Weekly Funnel Stock Change
    # --------------------------------------------------------------

    def plot_weekly_funnel_delta(
        self,
        output_path: Optional[Path] = None,
        show: bool = False,
    ):
        week_prev, week_curr = self.latest_two_snapshot_keys()

        query = f"""
        WITH weekly AS (
            SELECT snapshot_key, status, SUM(deal_count) AS deals
            FROM pipeline_snapshots
            GROUP BY snapshot_key, status
        )
        SELECT
            status,
            SUM(CASE WHEN snapshot_key = '{week_curr}' THEN deals ELSE 0 END)
          - SUM(CASE WHEN snapshot_key = '{week_prev}' THEN deals ELSE 0 END)
            AS delta
        FROM weekly
        GROUP BY status
        """

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn)

        df["status"] = pd.Categorical(
            df["status"], categories=FUNNEL_ORDER, ordered=True
        )
        df = df.sort_values("status")
        df = df[df["status"].notna()]

        colors = df["delta"].apply(
            lambda x: "#2ca02c" if x > 0 else "#d62728"
        )

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(df["status"], df["delta"], color=colors)
        ax.axvline(0, linewidth=1)

        ax.set_title(
            f"Weekly Funnel Stock Change ({week_curr} vs {week_prev})"
        )
        ax.set_xlabel("Net change in deal count")
        ax.set_ylabel("Funnel stage")

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=150, bbox_inches="tight")

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig, ax, week_prev, week_curr

    # --------------------------------------------------------------
    # Chart 3 — Funnel Stock (Centered)
    # --------------------------------------------------------------

    def plot_funnel_stock(
        self,
        snapshot_key: Optional[str] = None,
        output_path: Optional[Path] = None,
        show: bool = False,
    ):
        if snapshot_key is None:
            snapshot_key = self.latest_snapshot_key()

        query = """
        SELECT status, SUM(deal_count) AS deals
        FROM pipeline_snapshots
        WHERE snapshot_key = ?
        GROUP BY status
        """

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(snapshot_key,))

        df["status"] = pd.Categorical(
            df["status"], categories=FUNNEL_ORDER, ordered=True
        )
        df = df.sort_values("status")

        df["color"] = df["status"].map(STAGE_COLORS)

        max_val = df["deals"].max()
        lefts = -df["deals"] / 2

        fig, ax = plt.subplots(figsize=(8, 6))
        bars = ax.barh(
            df["status"],
            df["deals"],
            left=lefts,
            color=df["color"],
        )

        ax.set_xlim(-max_val * 0.55, max_val * 0.55)
        ax.set_xticks([])
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_title(f"Funnel Stock — {snapshot_key}")

        for bar, value in zip(bars, df["deals"]):
            ax.text(
                0,
                bar.get_y() + bar.get_height() / 2,
                f"{int(value)}",
                ha="center",
                va="center",
                fontweight="bold",
            )

        for spine in ax.spines.values():
            spine.set_visible(False)

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=150, bbox_inches="tight")

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig, ax, snapshot_key