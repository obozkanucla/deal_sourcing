import sqlite3
from datetime import date, timedelta
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"
RUN_DATE = date.today()
DRY_RUN = False


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
        (snapshot_key,)
    ).fetchone() is not None


def main():
    iso_year, iso_week, snapshot_key = get_snapshot_week(RUN_DATE)

    print("=" * 72)
    print("📊 Weekly Pipeline Snapshot")
    print(f"📀 SQLite DB : {DB_PATH}")
    print(f"🗓️ Week      : {snapshot_key}")
    print(f"🧪 DRY_RUN   : {DRY_RUN}")
    print("=" * 72)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if snapshot_exists(conn, snapshot_key):
        print("⛔ Snapshot already exists — aborting")
        return

    print("✅ No snapshot found — proceeding")
    rows = conn.execute(
        """
           SELECT
                COALESCE(industry, 'NA') AS industry,
                COALESCE(
                    status,
                    CASE
                        WHEN last_updated < DATE('now', '-7 days')
                            THEN 'not_yet_analysed'
                        ELSE 'new'
                    END
                ) AS status,
                source,
                COUNT(*) AS deal_count
            FROM deals
            GROUP BY
                industry,
                status,
                source;
        """
    ).fetchall()

    print(f"\n🧮 Snapshot rows generated: {len(rows)}")

    if DRY_RUN:
        print("\n🧪 DRY_RUN — snapshot NOT written")
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
                r["status"],
                r["source"],
                r["deal_count"],
                RUN_DATE.isoformat(),
            )
            for r in rows
        ]
    )

    conn.commit()
    conn.close()

    print("✅ Weekly snapshot written successfully")


if __name__ == "__main__":
    main()