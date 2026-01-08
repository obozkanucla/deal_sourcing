import sqlite3
from pathlib import Path
from src.integrations.drive_folders import CANONICAL_INDUSTRY_FOLDERS
from src.integrations.google_drive import get_drive_service
from googleapiclient.errors import HttpError

# =========================================================
# CONFIG
# =========================================================

BROKER_NAME = "DealOpportunities"
DRY_RUN = False   # ⛔ SET TO False TO ACTUALLY DELETE
DELETE_SQL = False

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

def cleanup_broker_sqlite(broker: str, dry_run: bool = True):
    print(f"\n🧹 SQLite cleanup for broker='{broker}'")
    print(f"🧪 DRY_RUN={dry_run}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Count first (always)
    cur = conn.execute(
        "SELECT COUNT(*) AS n FROM deals WHERE source = ?",
        (broker,),
    )
    count = cur.fetchone()["n"]

    print(f"  → Deals to delete: {count}")

    if count == 0:
        conn.close()
        print("  → Nothing to delete")
        return

    if dry_run:
        print("  🧪 DRY RUN — no rows deleted")
        conn.close()
        return

    # ---- ACTUAL DELETE ----
    conn.execute(
        "DELETE FROM deals WHERE source = ?",
        (broker,),
    )

    conn.commit()
    conn.close()

    print(f"  ✅ Deleted {count} deals from SQLite")


# =========================================================
# DRIVE HELPERS
# =========================================================


def list_subfolders(service, parent_id: str):
    q = (
        f"'{parent_id}' in parents "
        "and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )

    res = service.files().list(
        q=q,
        fields="files(id, name)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    return res.get("files", [])


def delete_folder(service, folder_id: str):
    try:
        service.files().delete(
            fileId=folder_id,
            supportsAllDrives=True,
        ).execute()
    except HttpError as e:
        if e.resp.status == 404:
            print(f"  ⚠️ Folder already gone or inaccessible (id={folder_id})")
        else:
            raise


def trash_folder(service, folder_id: str):
    try:
        service.files().update(
            fileId=folder_id,
            body={"trashed": True},
            supportsAllDrives=True,
        ).execute()
    except HttpError as e:
        if e.resp.status in (403, 404):
            print(
                f"  ⚠️ Cannot trash folder (insufficient permissions or already gone): "
                f"id={folder_id}"
            )
        else:
            raise

# =========================================================
# MAIN
# =========================================================

def main():
    service = get_drive_service()

    print(f"\n🧹 Cleaning broker='{BROKER_NAME}' across industries")
    print(f"🧪 DRY_RUN={DRY_RUN}\n")
    if DELETE_SQL:
        cleanup_broker_sqlite(BROKER_NAME, dry_run=DRY_RUN)
    deleted = 0

    for industry, parent_id in CANONICAL_INDUSTRY_FOLDERS.items():
        print(f"📁 Industry: {industry}")

        subfolders = list_subfolders(service, parent_id)

        broker_folders = [
            f for f in subfolders
            if f["name"] == BROKER_NAME
        ]

        if not broker_folders:
            print("  └─ No broker folder")
            continue

        for folder in broker_folders:
            folder_id = folder["id"]
            folder_name = folder["name"]

            if DRY_RUN:
                print(
                    f"  🧪 WOULD DELETE: "
                    f"{industry} / {folder_name} (id={folder_id})"
                )
            else:
                print(
                    f"  🗑️ DELETING: "
                    f"{industry} / {folder_name} (id={folder_id})"
                )
                # delete_folder(service, folder_id)
                trash_folder(service, folder_id)
            deleted += 1

    print(
        f"\n✅ Cleanup complete — broker folders targeted: {deleted}"
    )

    if DRY_RUN:
        print("⚠️ DRY_RUN enabled — no folders were deleted")


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    main()