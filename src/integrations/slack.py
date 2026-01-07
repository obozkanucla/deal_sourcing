import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_DEFAULT_CHANNEL = os.getenv("SLACK_CHANNEL", "#deal-alerts")
SLACK_DEFAULT_CHANNEL_ID = os.getenv("SLACK_DEFAULT_CHANNEL_ID")

class SlackNotifier:
    def __init__(self, token: str | None = None, channel: str | None = None,
                 channel_id: str | None = None):
        self.client = WebClient(token=token or SLACK_BOT_TOKEN)
        self.channel = channel or SLACK_DEFAULT_CHANNEL
        self.channel_id = channel_id or SLACK_DEFAULT_CHANNEL_ID
        if not SLACK_BOT_TOKEN:
            raise RuntimeError("SLACK_BOT_TOKEN not set")

    # -------------------------------------------------
    # TEXT MESSAGE
    # -------------------------------------------------
    def send_message(self, title: str, text: str, level: str = "info"):
        emoji = {
            "info": ":information_source:",
            "success": ":white_check_mark:",
            "warning": ":warning:",
            "error": ":x:",
        }.get(level, ":speech_balloon:")

        try:
            self.client.chat_postMessage(
                channel=self.channel_id,
                text=f"{emoji} *{title}*\n{text}",
            )
        except SlackApiError as e:
            raise RuntimeError(
                f"Slack message failed: {e.response['error']}"
            )

    # -------------------------------------------------
    # FILE UPLOAD (charts, reports)
    # -------------------------------------------------
    def upload_file(
        self,
        file_path: str,
        title: str,
        initial_comment: str | None = None,
    ):
        try:
            self.client.files_upload_v2(
                channel=self.channel,
                file=file_path,
                title=title,
                initial_comment=initial_comment,
            )
        except SlackApiError as e:
            raise RuntimeError(
                f"Slack file upload failed: {e.response['error']}"
            )