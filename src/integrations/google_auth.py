from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from pathlib import Path

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

def get_google_credentials():

    # Resolve project root (…/deal_sourcing)
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    token_path = PROJECT_ROOT / "config/google/token.pickle"
    creds_path = PROJECT_ROOT / "config/google/credentials.json"

    creds = None

    if token_path.exists():
        with open(token_path, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_path,
                scopes=DEFAULT_SCOPES,
            )
            creds = flow.run_local_server(port=0)

        with open(token_path, "wb") as token:
            pickle.dump(creds, token)

    return creds