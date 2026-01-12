import sqlite3
from datetime import date, timedelta
from pathlib import Path

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def week_start(d):
    return d - timedelta(days=d.weekday())

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
RUN_DATE = date.today()
DRY_RUN = False

SNAPSHOT_WEEK_START = week_start(RUN_DATE).isoformat()

# -------------------------------------------------
# SNAPSHOT HELPERS
# -------------------------------------------------

def get_snapshot_week(d: date):
    iso_year, iso_week, _ = d.isocalendar()
    snapshot_key = f"{iso_year}-W{iso_week:02d}"
    return iso_year, iso_week, snapshot_key


def snapshot_exists(conn, snapshot_key: str) -> bool:
    return conn.execute(
        """
        SELECT 1
        FROM pipeline_snapshots
        WHERE snapshot_key = ?
        LIMIT 1
        """,
        (snapshot_key,),
    ).fetchone() is not None


# -------------------------------------------------
# MAIN SNAPSHOT RUNNER
# -------------------------------------------------

def snapshot_pipeline_run(force_current_week=False):
    iso_year, iso_week, snapshot_key = get_snapshot_week(RUN_DATE)

    print("=" * 72)
    print("📊 Weekly Pipeline Snapshot")
    print(f"📀 SQLite DB : {DB_PATH}")
    print(f"🗓️ Week      : {snapshot_key}")
    print(f"🧪 DRY_RUN   : {DRY_RUN}")
    print(f"📅 Week start: {SNAPSHOT_WEEK_START}")
    print("=" * 72)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if snapshot_exists(conn, snapshot_key):
        if not force_current_week:
            print("⛔ Snapshot already exists — skipping")
            conn.close()
            return
        else:
            print("♻️ Overwriting snapshot for", snapshot_key)
            conn.execute(
                "DELETE FROM pipeline_snapshots WHERE snapshot_key = ?",
                (snapshot_key,),
            )
            conn.commit()

    rows = conn.execute(
        """
        SELECT
            COALESCE(industry, 'NA') AS industry,

            CASE
                -- assessed deals
                WHEN status = 'Initial Contact' THEN 'Initial Contact'
                WHEN status = 'CIM' THEN 'CIM'
                WHEN status = 'CIM DD' THEN 'CIM DD'
                WHEN status = 'Meeting' THEN 'Meeting'
                WHEN status = 'LOI' THEN 'LOI'
                WHEN status = 'Pass' THEN 'Pass'
                WHEN status = 'Lost' THEN 'Lost'

                -- NEW: first seen during snapshot week
                WHEN status IS NULL
                     AND first_seen IS NOT NULL
                     AND DATE(first_seen) >= DATE(?)
                    THEN 'New'

                -- UNASSESSED: backlog (includes NULL first_seen)
                WHEN status IS NULL
                    THEN 'Unassessed'

                ELSE NULL
            END AS snapshot_status,

            source,
            COUNT(*) AS deal_count
        FROM deals
        GROUP BY
            industry,
            snapshot_status,
            source
        HAVING
            snapshot_status IS NOT NULL;
        """,
        (SNAPSHOT_WEEK_START,),
    ).fetchall()

    print(f"\n🧮 Snapshot rows generated: {len(rows)}")

    if DRY_RUN:
        print("\n🧪 DRY_RUN — snapshot NOT written")
        conn.close()
        return

    conn.executemany(
        """
        INSERT INTO pipeline_snapshots (
            snapshot_year,
            snapshot_week,
            snapshot_key,
            industry,
            status,
            source,
            deal_count,
            snapshot_run_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                iso_year,
                iso_week,
                snapshot_key,
                r["industry"],
                r["snapshot_status"],
                r["source"],
                r["deal_count"],
                RUN_DATE.isoformat(),
            )
            for r in rows
        ],
    )

    conn.commit()
    conn.close()

    print("✅ Weekly snapshot written successfully")


# -------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------

if __name__ == "__main__":
    snapshot_pipeline_run(force_current_week=True)