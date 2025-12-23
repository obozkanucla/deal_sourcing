from pathlib import Path
import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

BASE_DIR = Path(__file__).resolve().parents[2]
CREDS_PATH = BASE_DIR / "config/google/credentials.json"
TOKEN_PATH = BASE_DIR / "config/google/token.pickle"


def get_gspread_client():
    creds = None

    if TOKEN_PATH.exists():
        with open(TOKEN_PATH, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDS_PATH,
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "wb") as f:
            pickle.dump(creds, f)

    return gspread.authorize(creds)