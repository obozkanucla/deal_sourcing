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

# google_drive.py

def find_or_create_deal_folder(
    *,
    parent_folder_id: str,
    deal_id: str,
    deal_title: str | None = None,
    month_prefix: str | None = None,
) -> str:
    service = get_drive_service()

    if not deal_id:
        raise RuntimeError("deal_id is required")

    # ---- MONTH PREFIX ----
    if month_prefix is None:
        month_prefix = datetime.utcnow().strftime("%y%m")

    # Defensive: ensure format stays sane if overridden
    month_prefix = month_prefix.strip()

    # ---- SAFE TITLE ----
    safe_title = (deal_title or "").strip()

    # HARD truncate to keep UI sane
    if safe_title:
        safe_title = safe_title[:40]

    folder_name = (
        f"{month_prefix} [{deal_id}] {safe_title}"
        if safe_title
        else f"{month_prefix} [{deal_id}]"
    )

    # ---- IDENTITY-BASED SEARCH ----
    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name contains '[{deal_id}]' "
        f"and '{parent_folder_id}' in parents "
        "and trashed=false"
    )

    res = service.files().list(
        q=q,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    ).execute()

    files = res.get("files", [])

    if len(files) > 1:
        raise RuntimeError(
            f"Multiple folders found for deal_id={deal_id}: "
            + ", ".join(f["name"] for f in files)
        )

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
# Folder move
# -------------------------------------------------

def move_folder_to_parent(folder_id: str, new_parent_id: str):
    """
    Move an existing Drive folder to a new parent folder.
    Keeps the same Drive ID and URL.
    """
    service = get_drive_service()

    file = service.files().get(
        fileId=folder_id,
        fields="parents",
        supportsAllDrives=True,
    ).execute()

    previous_parents = ",".join(file.get("parents", []))

    service.files().update(
        fileId=folder_id,
        addParents=new_parent_id,
        removeParents=previous_parents,
        supportsAllDrives=True,
    ).execute()

# -------------------------------------------------
# PDF upload
# -------------------------------------------------


def find_existing_pdf(*, folder_id: str, filename: str) -> str | None:
    service = get_drive_service()

    q = (
        "mimeType='application/pdf' "
        f"and name='{filename}' "
        f"and '{folder_id}' in parents "
        "and trashed=false"
    )

    res = service.files().list(
        q=q,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    ).execute()

    files = res.get("files", [])
    if len(files) > 1:
        raise RuntimeError(
            f"Multiple PDFs named '{filename}' in folder {folder_id}"
        )

    return files[0]["id"] if files else None

def upload_pdf_to_drive(
    *,
    local_path: Path,
    filename: str,
    folder_id: str,
) -> str:
    service = get_drive_service()

    media = MediaFileUpload(
        str(local_path),
        mimetype="application/pdf",
        resumable=True,
    )

    existing_file_id = find_existing_pdf(
        folder_id=folder_id,
        filename=filename,
    )

    if existing_file_id:
        # 🔁 REPLACE CONTENT
        file = service.files().update(
            fileId=existing_file_id,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
    else:
        # ➕ CREATE NEW
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

def discover_existing_drive_pdfs(conn, deal, folder_id):
    """
    Reconcile existing Drive PDFs into deal_artifacts + deals table.
    """
    files = list_files_in_folder(folder_id)

    for f in files:
        name = f["name"].lower()
        drive_url = f["webViewLink"]
        file_id = f["id"]

        if name.endswith("-listing.pdf"):
            artifact_type = "listing_pdf"
        elif name.endswith(".pdf"):
            artifact_type = "information_memorandum"
        else:
            continue

        existing = conn.execute(
            """
            SELECT 1 FROM deal_artifacts
            WHERE deal_id = ?
              AND artifact_type = ?
              AND drive_file_id = ?
            """,
            (deal["id"], artifact_type, file_id),
        ).fetchone()

        if existing:
            continue

        record_deal_artifact(
            conn=conn,
            source=SOURCE,
            source_listing_id=deal["source_listing_id"],
            deal_id=deal["id"],
            artifact_type=artifact_type,
            artifact_name=f["name"],
            artifact_hash=None,  # unknown, Drive-originated
            drive_file_id=file_id,
            drive_url=drive_url,
            extraction_version="drive-reconcile",
            created_by="enrich_abercorn.py",
        )

        if artifact_type == "information_memorandum":
            conn.execute(
                """
                UPDATE deals
                SET pdf_drive_url = ?,
                    last_updated = CURRENT_TIMESTAMP,
                    last_updated_source = 'AUTO'
                WHERE id = ?
                """,
                (drive_url, deal["id"]),
            )

    conn.commit()