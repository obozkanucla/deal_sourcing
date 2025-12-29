from datetime import datetime
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from src.integrations.google_auth import get_google_credentials


# -------------------------------------------------
# Service
# -------------------------------------------------

def get_drive_service():
    creds = get_google_credentials()
    return build("drive", "v3", credentials=creds)


# -------------------------------------------------
# Deal folder (UNDER KNOWN PARENT)
# -------------------------------------------------

def find_or_create_deal_folder(
    *,
    parent_folder_id: str,
    deal_id: str,
    deal_title: str | None = None,
) -> str:
    """
    Creates or finds a deal folder under a known parent.
    Parent is expected to be broker-level.
    """
    service = get_drive_service()

    month_prefix = datetime.utcnow().strftime("%y%m")
    folder_name = f"{month_prefix} {deal_title or deal_id}"

    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{folder_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed=false"
    )

    res = service.files().list(
        q=q,
        fields="files(id, name)",
        supportsAllDrives=True,
    ).execute()

    files = res.get("files", [])
    if files:
        return files[0]["id"]

    folder = service.files().create(
        body={
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()

    return folder["id"]


# -------------------------------------------------
# PDF upload
# -------------------------------------------------

def upload_pdf_to_drive(
    *,
    local_path: Path,
    filename: str,
    folder_id: str,
) -> str:
    """
    Upload PDF into deal folder.
    Returns Drive file URL.
    """
    service = get_drive_service()

    media = MediaFileUpload(
        str(local_path),
        mimetype="application/pdf",
        resumable=True,
    )

    file = service.files().create(
        body={
            "name": filename,
            "parents": [folder_id],
        },
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()

    return f"https://drive.google.com/file/d/{file['id']}/view"