import os
from pathlib import Path
from typing import Iterable, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root
load_dotenv()

class SlackReporter:
    """
    Thin Slack transport layer.

    Responsibilities:
    - Send messages to Slack
    - Upload one or more files
    - No knowledge of *what* the report is
    - No formatting or business logic
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        channel_id: Optional[str] = None,
    ):
        self.bot_token = bot_token or os.getenv("SLACK_BOT_TOKEN")
        self.channel_id = channel_id or os.getenv("SLACK_DEFAULT_CHANNEL_ID")

        if not self.bot_token:
            raise RuntimeError("SLACK_BOT_TOKEN not set")

        if not self.channel_id:
            raise RuntimeError("SLACK_DEFAULT_CHANNEL_ID not set")

        self.client = WebClient(token=self.bot_token)

    # --------------------------------------------------
    # Messaging
    # --------------------------------------------------

    def post_message(self, text: str) -> None:
        try:
            self.client.chat_postMessage(
                channel=self.channel_id,
                text=text,
            )
        except SlackApiError as e:
            raise RuntimeError(
                f"Slack message failed: {e.response['error']}"
            )

    # --------------------------------------------------
    # File uploads
    # --------------------------------------------------

    def upload_file(
        self,
        file_path: Path,
        title: Optional[str] = None,
        initial_comment: Optional[str] = None,
    ) -> None:
        if not file_path.exists():
            raise FileNotFoundError(file_path)

        try:
            self.client.files_upload_v2(
                channel=self.channel_id,
                file=str(file_path),
                title=title,
                initial_comment=initial_comment,
            )
        except SlackApiError as e:
            raise RuntimeError(
                f"Slack file upload failed: {e.response['error']}"
            )

    def upload_files(
        self,
        files: Iterable[Path],
        header_message: Optional[str] = None,
    ) -> None:
        """
        Upload multiple files sequentially.
        The header_message (if provided) is attached to the first file only.
        """
        files = list(files)
        if not files:
            return

        for i, path in enumerate(files):
            self.upload_file(
                file_path=path,
                initial_comment=header_message if i == 0 else None,
            )