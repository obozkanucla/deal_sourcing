"""
Authoritative Google Drive folder resolution.

RULES (HARD):
- Industry folders MUST exist (configured below)
- Broker folders are auto-created if missing
- Sector folders are NOT used
- Only deal-level folders are auto-created
"""

from datetime import datetime
from googleapiclient.discovery import build
from src.integrations.google_auth import get_google_credentials

# ============================================================
# CANONICAL INDUSTRY → GOOGLE DRIVE FOLDER IDS (MUST EXIST)
# ============================================================

CANONICAL_INDUSTRY_FOLDERS = {
    "Business_Services":                "1iQWqPeKjFTJ-8G37RO5icUolwn8j-A4H",
    "Construction_Built_Environment":   "1aJO_v6NVD5-FoszjSuqJKDoWwGkjSY9B",
    "Consumer_Retail":                  "1PGAMQe3sC9uar0a2Rxf69dS6IgSQpEjp",
    "Education":                        "1uV_mfsxPWUWqbw3Fbm6xAXvk2n_ybGsM",
    "Financial_Services":               "1CNjQbT0NDBCVYJGRUWPQIWuzItgExIxf",
    "Food_Beverage":                    "1AmoX8rqqWsvMLhXPyWioZ8tgPvdIeIrB",
    "Industrials":                      "1uCtveqGZjibosmiDnH74FOKHnNMuBBvN",
    "Logistics_Distribution":           "1m1CWlNI74DkcUVT2gG-RcrSlsFwHV11T",
    "Technology":                       "1KdA-m9R6gjICTxRO0Sxd_3VAMu4aGZMy",
    "Healthcare":                       "1BwuX7y1sQFprgoYkctA09nUsfiUUPrbj",
    "Other":                            "1RKZ-FY6PQIxVVd2Rv4x1zSEIM59S-z9k",
    "Agriculture":                      "1WAzP0mcZOsdMyGigBsYlQDQKFk_Yeb_h",
    "Franchise_Businesses":             "1j2WaQe1JbyRbqQ_2AUzuSy7HphPTAPn4"
}

# ============================================================
# GOOGLE DRIVE SERVICE
# ============================================================

def get_drive_service():
    creds = get_google_credentials()
    return build("drive", "v3", credentials=creds)

# ============================================================
# INTERNAL: FIND OR CREATE FOLDER
# ============================================================

def _find_or_create_folder(*, parent_id: str, name: str) -> str:
    service = get_drive_service()

    query = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{name}' "
        f"and '{parent_id}' in parents "
        "and trashed=false"
    )

    res = service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    ).execute()

    files = res.get("files", [])

    if files:
        return files[0]["id"]

    folder = service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()

    return folder["id"]

# ============================================================
# PUBLIC: DRIVE PARENT RESOLUTION
# ============================================================

def get_drive_parent_folder_id(
    *,
    industry: str,
    broker: str,
) -> str:
    """
    Returns the BROKER folder ID under the given industry.
    Creates broker folder if missing.
    """

    if industry not in CANONICAL_INDUSTRY_FOLDERS:
        raise KeyError(f"No Drive folder configured for industry='{industry}'")

    industry_folder_id = CANONICAL_INDUSTRY_FOLDERS[industry]

    broker_folder_id = _find_or_create_folder(
        parent_id=industry_folder_id,
        name=broker,
    )

    return broker_folder_id