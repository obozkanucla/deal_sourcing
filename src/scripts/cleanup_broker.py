"""
ONE-BUTTON BROKER RESET

What this script does:
1. Deletes ALL deals for a broker from SQLite
2. Recursively deletes ALL Google Drive folders/files for that broker
   across all industry folders (Shared Drives safe)

⚠️ Shared Drive rules:
- Parent folders cannot be deleted unless empty
- We must delete children bottom-up (recursive)

SET DRY_RUN=False TO ACTUALLY DELETE
"""

from pathlib import Path
import sqlite3
from typing import List

from googleapiclient.errors import HttpError

from src.integrations.google_drive import get_drive_service
from src.integrations.drive_folders import CANONICAL_INDUSTRY_FOLDERS

# =========================================================
# CONFIG
# =========================================================

BROKER_NAME = "DealOpportunities"
DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = False   # ⛔ SET TO False TO ACTUALLY DELETE
SQL_DELETE = False

# =========================================================
# SQLITE CLEANUP
# =========================================================

def cleanup_sqlite(broker: str):
    print("\n🧹 SQLite cleanup")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*) FROM deals
        WHERE source = ?
        """,
        (broker,),
    )
    count = cur.fetchone()[0]

    print(f"  Found {count} deals for broker='{broker}'")

    if DRY_RUN or not SQL_DELETE:
        print("  🧪 DRY_RUN — no rows deleted")
    else:
        cur.execute(
            """
            DELETE FROM deals
            WHERE source = ?
            """,
            (broker,),
        )
        conn.commit()
        print(f"  🗑️ Deleted {count} rows")

    conn.close()


# =========================================================
# GOOGLE DRIVE HELPERS
# =========================================================

def list_children(service, parent_id: str) -> List[dict]:
    """
    List ALL immediate children (files + folders) of a Drive folder.
    Works for Shared Drives.
    """
    q = f"'{parent_id}' in parents and trashed = false"

    results = []
    page_token = None

    while True:
        res = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_token,
            pageSize=1000,
        ).execute()

        results.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return results


def delete_tree(service, folder_id: str, indent: int = 0):
    """
    Recursively delete all children of a folder (depth-first).
    """
    children = list_children(service, folder_id)

    for item in children:
        name = item["name"]
        item_id = item["id"]
        mime = item["mimeType"]

        prefix = "  " * indent

        if mime == "application/vnd.google-apps.folder":
            print(f"{prefix}📂 Entering folder: {name}")
            delete_tree(service, item_id, indent + 1)

        print(f"{prefix}🗑️ Deleting: {name}")

        if not DRY_RUN:
            try:
                service.files().delete(
                    fileId=item_id,
                    supportsAllDrives=True,
                ).execute()
            except HttpError as e:
                print(f"{prefix}⚠️ Failed to delete {name}: {e}")


# =========================================================
# DRIVE CLEANUP
# =========================================================

def cleanup_drive(broker: str):
    print("\n🧹 Google Drive cleanup")
    service = get_drive_service()

    deleted_roots = 0

    for industry, industry_root_id in CANONICAL_INDUSTRY_FOLDERS.items():
        print(f"\n📁 Industry: {industry}")

        children = list_children(service, industry_root_id)

        broker_folders = [
            f for f in children
            if f["name"] == broker
            and f["mimeType"] == "application/vnd.google-apps.folder"
        ]

        if not broker_folders:
            print("  └─ No broker folder")
            continue

        for folder in broker_folders:
            folder_id = folder["id"]

            print(f"  📂 Broker folder found: {broker}")

            # SAFETY ASSERT
            assert folder["name"] == broker

            delete_tree(service, folder_id, indent=2)

            print(f"  🗑️ Deleting broker root folder")

            if not DRY_RUN:
                try:
                    service.files().delete(
                        fileId=folder_id,
                        supportsAllDrives=True,
                    ).execute()
                except HttpError as e:
                    print(f"  ⚠️ Failed to delete broker folder: {e}")

            deleted_roots += 1

    print(
        f"\n✅ Drive cleanup complete — broker folders targeted: {deleted_roots}"
    )

    if DRY_RUN:
        print("⚠️ DRY_RUN enabled — no Drive data was deleted")


# =========================================================
# ENTRYPOINT
# =========================================================

def main():
    print("=" * 60)
    print(f"🚨 BROKER RESET: {BROKER_NAME}")
    print(f"🧪 DRY_RUN={DRY_RUN}")
    print("=" * 60)

    cleanup_sqlite(BROKER_NAME)
    cleanup_drive(BROKER_NAME)

    print("\n🏁 Broker reset finished")


if __name__ == "__main__":
    main()