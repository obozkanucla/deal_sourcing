from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from src.integrations.google_auth import get_google_credentials


def upload_pdf_to_drive(pdf_path, folder_id):
    creds = get_google_credentials()
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": pdf_path.name,
        "parents": [folder_id],
    }

    media = MediaFileUpload(
        str(pdf_path),
        mimetype="application/pdf",
        resumable=True,
    )

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink",
    ).execute()

    return file["webViewLink"]