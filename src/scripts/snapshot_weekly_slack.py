import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pathlib import Path

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

SLACK_CHANNEL = os.getenv("SLACK_DEFAULT_CHANNEL_ID")


def post_snapshot_to_slack(snapshot_key: str, image_path: Path):
    print(SLACK_BOT_TOKEN)
    if not SLACK_BOT_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN not set")

    client = WebClient(token=SLACK_BOT_TOKEN)

    message = (
        f"📊 *Weekly Pipeline Snapshot — {snapshot_key}*\n\n"
        "• Deals by *Industry × Status*\n"
        "• NA = not yet industry-classified\n"
        "• Generated automatically\n"
    )

    try:
        client.files_upload(
            channels=SLACK_CHANNEL,
            file=str(image_path),
            title=f"Pipeline Snapshot {snapshot_key}",
            initial_comment=message,
        )
    except SlackApiError as e:
        # Do not crash the pipeline for Slack issues
        print(f"⚠️ Slack upload failed: {e.response['error']}")