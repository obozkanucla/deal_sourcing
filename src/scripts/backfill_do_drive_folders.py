import sqlite3
from pathlib import Path

from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import find_or_create_deal_folder

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

def backfill_do_drive_folders():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            id,
            source_listing_id,
            title,
            industry
        FROM deals
        WHERE source = 'DealOpportunities'
          AND drive_folder_url IS NULL
        """
    ).fetchall()

    total = len(rows)
    print(f"🔧 Backfilling Drive folders for {total} DO deals")

    if not rows:
        print("✅ Nothing to backfill")
        return

    for idx, r in enumerate(rows, start=1):
        deal_id = r["id"]
        deal_key = r["source_listing_id"]
        title = r["title"] or deal_key
        industry = r["industry"]

        print(f"📁 [{idx}/{total}] {deal_key}")

        parent_folder_id = get_drive_parent_folder_id(
            industry=industry,
            broker="DealOpportunities",
        )

        folder_id = find_or_create_deal_folder(
            parent_folder_id=parent_folder_id,
            deal_id=deal_key,
            deal_title=title,
        )

        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

        conn.execute(
            """
            UPDATE deals
            SET
                drive_folder_id  = ?,
                drive_folder_url = ?,
                last_updated     = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (folder_id, folder_url, deal_id),
        )
        conn.commit()

    conn.close()
    print("🏁 Drive folder backfill complete")

if __name__ == "__main__":
    backfill_do_drive_folders()