"""Slack helpers for athena_capacity_reservation."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slack_sdk import WebClient

logger = logging.getLogger(__name__)

# In-process thread state: once the first message is posted,
# subsequent messages are threaded under the same ts.
_current_thread_ts: str | None = None


def get_slack_client(slack_token: str | None) -> "WebClient | None":
    """Get the Slack WebClient from the provided token."""
    try:
        from slack_sdk import WebClient
    except ImportError:
        logger.warning("slack_sdk is not installed. Skipping Slack notification.")
        return None

    if not slack_token:
        logger.warning("slack_token is not set. Skipping Slack notification.")
        return None
    return WebClient(token=slack_token, timeout=30)


def post_slack_message(
    message: str,
    color: str,
    slack_token: str | None = None,
    slack_channel: str | None = None,
    slack_thread_ts: str | None = None,
) -> bool:
    """Post a message to Slack.

    Threading behaviour:
    - If slack_thread_ts is provided, replies into that thread.
    - Otherwise, the first call creates a new message and remembers its ts
      so that subsequent calls within the same process are threaded.

    Returns True on success, False on skip or error.
    """
    global _current_thread_ts  # noqa: PLW0603

    if not slack_channel:
        logger.warning("slack_channel is not set. Skipping Slack notification.")
        return False

    client = get_slack_client(slack_token)
    if client is None:
        return False

    try:
        from slack_sdk.errors import SlackApiError

        thread_ts = _current_thread_ts or slack_thread_ts

        attachments = [
            {
                "color": color,
                "fallback": message,
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": message}}],
            }
        ]

        if thread_ts:
            resp = client.chat_postMessage(
                channel=slack_channel,
                text=message,
                attachments=attachments,
                thread_ts=thread_ts,
            )
        else:
            resp = client.chat_postMessage(
                channel=slack_channel,
                text=message,
                attachments=attachments,
            )

        if _current_thread_ts is None:
            ts = resp.get("ts")
            if isinstance(ts, str):
                _current_thread_ts = thread_ts or ts

        logger.info("Posted Slack message: %s", message)
        return True
    except SlackApiError as e:
        logger.error("SlackApiError in post_slack_message: %s", e)
        return False
    except (OSError, ValueError) as e:
        logger.error("Error in post_slack_message: %s", e)
        return False
