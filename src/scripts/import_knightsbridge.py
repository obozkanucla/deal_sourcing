import sqlite3
from pathlib import Path
from datetime import date

from src.brokers.knightsbridge_client import KnightsbridgeClient


DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"


def import_knightsbridge():
    print(f"📀 SQLite DB path: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    client = KnightsbridgeClient()
    client.start()

    try:
        rows = client.fetch_index()

        today = date.today().isoformat()
        inserted = 0
        refreshed = 0

        for row in rows:
            cur = conn.execute(
                """
                SELECT id
                FROM deals
                WHERE source = ?
                  AND source_listing_id = ?
                """,
                (row["source"], row["source_listing_id"]),
            ).fetchone()

            if cur:
                conn.execute(
                    """
                    UPDATE deals
                    SET
                        last_seen = ?,
                        title = COALESCE(title, ?),
                        source_url = ?
                    WHERE id = ?
                    """,
                    (
                        today,
                        row["title"],
                        row["source_url"],
                        cur["id"],
                    ),
                )
                refreshed += 1
            else:
                conn.execute(
                    """
                    INSERT INTO deals (
                        source,
                        source_listing_id,
                        source_url,
                        title,
                        sector_raw,
                        first_seen,
                        last_seen,
                        needs_detail_refresh
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        row["source"],
                        row["source_listing_id"],
                        row["source_url"],
                        row["title"],
                        row["sector_raw"],
                        today,
                        today,
                    ),
                )
                inserted += 1

        conn.commit()

        print(
            f"✅ Knightsbridge import complete — "
            f"inserted={inserted}, refreshed={refreshed}"
        )

    finally:
        client.stop()
        conn.close()


if __name__ == "__main__":
    import_knightsbridge()