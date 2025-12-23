from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
from src.integrations.google_auth import get_google_credentials
from src.integrations.drive_folders import BROKER_FOLDERS
from datetime import datetime

def upload_pdf_to_drive(local_path, filename, folder_id):
    creds = get_google_credentials()
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }
    media = MediaFileUpload(local_path, mimetype="application/pdf")

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()

    return f"https://drive.google.com/file/d/{file['id']}/view"

def get_drive_service():
    creds = get_google_credentials()
    return build("drive", "v3", credentials=creds)


def find_or_create_deal_folder(*, parent_folder_id, deal_id, deal_title=None):
    service = get_drive_service()

    month_prefix = datetime.utcnow().strftime("%y%m")
    name_parts = [month_prefix, deal_title or deal_id]
    folder_name = " ".join(name_parts)

    # 1️⃣ Check if folder already exists
    q = (
        f"mimeType='application/vnd.google-apps.folder' "
        f"and name='{folder_name}' "
        f"and '{parent_folder_id}' in parents "
        f"and trashed=false"
    )

    res = service.files().list(q=q, fields="files(id, name)").execute()
    files = res.get("files", [])

    if files:
        return files[0]["id"]

    # 2️⃣ Create folder
    folder = service.files().create(
        body={
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        },
        fields="id",
        supportsAllDrives=True,  # ✅ REQUIRED
    ).execute()

    return folder["id"]