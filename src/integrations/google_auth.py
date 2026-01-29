import os
import pickle
from pathlib import Path

import google.auth
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

def get_google_credentials():
    """
    Credential resolution order:

    1. CI → Workload Identity Federation (google.auth.default)
    2. Local → token.pickle (cached user OAuth)
    3. Local → interactive OAuth (fallback)
    """

    # --- CI MODE: Workload Identity Federation ---
    if os.getenv("CI") == "true":
        creds, _ = google.auth.default(scopes=DEFAULT_SCOPES)
        return creds

    # --- LOCAL / DEV MODE ---
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    token_path = PROJECT_ROOT / "config/google/token.pickle"
    creds_path = PROJECT_ROOT / "config/google/credentials.json"

    creds = None

    if token_path.exists():
        with open(token_path, "rb") as token:
            creds = pickle.load(token)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "wb") as token:
            pickle.dump(creds, token)
        return creds

    # --- LOCAL ONLY: interactive OAuth ---
    flow = InstalledAppFlow.from_client_secrets_file(
        creds_path,
        scopes=DEFAULT_SCOPES,
    )
    creds = flow.run_local_server(port=0)

    with open(token_path, "wb") as token:
        pickle.dump(creds, token)

    return creds

def get_google_refresh_token():
    from google_auth_oauthlib.flow import InstalledAppFlow
    # Resolve project root (…/deal_sourcing)
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    token_path = PROJECT_ROOT / "config/google/token.pickle"
    creds_path = PROJECT_ROOT / "config/google/credentials.json"

    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    flow = InstalledAppFlow.from_client_secrets_file(
        creds_path,
        scopes=SCOPES,
    )

    creds = flow.run_local_server(port=0)

    print("REFRESH TOKEN:")
    print(creds.refresh_token)

if __name__ == "__main__":
    get_google_refresh_token()