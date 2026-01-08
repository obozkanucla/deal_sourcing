from src.integrations.google_drive import get_drive_service
from src.integrations.drive_folders import CANONICAL_INDUSTRY_FOLDERS


def list_child_folders(drive, parent_id: str):
    q = (
        f"'{parent_id}' in parents and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )

    res = drive.files().list(
        q=q,
        fields="files(id, name)",
        pageSize=100,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    return res.get("files", [])


def main():
    drive = get_drive_service()

    print("\n📂 LISTING INDUSTRY SUBFOLDERS (RAW DRIVE VIEW)\n")

    for industry, industry_folder_id in CANONICAL_INDUSTRY_FOLDERS.items():
        print(f"📁 Industry: {industry}")
        print(f"    parent_id = {industry_folder_id}")

        children = list_child_folders(drive, industry_folder_id)

        if not children:
            print("    └─ ❌ NO SUBFOLDERS FOUND")
            continue

        for f in children:
            print(f"    └─ '{f['name']}'  (id={f['id']})")

        print()

    print("✅ Done")


if __name__ == "__main__":
    main()