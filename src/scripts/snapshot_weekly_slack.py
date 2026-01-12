from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pathlib import Path
import os
load_dotenv()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

SLACK_CHANNEL = os.getenv("SLACK_DEFAULT_CHANNEL_ID")

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

SLACK_CHANNEL_ID = os.getenv("SLACK_DEFAULT_CHANNEL_ID")


def post_snapshot_to_slack(snapshot_key: str, image_path: Path):
    if not SLACK_BOT_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN not set")

    client = WebClient(token=SLACK_BOT_TOKEN)

    message = (
        f"📊 *Weekly Pipeline Snapshot — {snapshot_key}*\n\n"
        "• Deals by *Industry × Status*\n"
        "• NA = not yet industry-classified\n"
        "• Generated automatically"
    )

    try:
        client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            file=str(image_path),
            title=f"Pipeline Snapshot {snapshot_key}",
            initial_comment=message,
        )
    except SlackApiError as e:
        print(f"⚠️ Slack upload failed: {e.response['error']}")