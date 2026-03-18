"""Slack helpers for athena_capacity_reservation."""


import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from slack_sdk import WebClient

logger = logging.getLogger(__name__)


class _SlackState(TypedDict, total=False):
    thread_ts: str


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


def load_state(state_file: Path) -> _SlackState:
    """Load Slack notification state from JSON file."""
    if not state_file.exists():
        return _SlackState()
    try:
        data = json.loads(state_file.read_text())
        state = _SlackState()
        if isinstance(data, dict) and isinstance(data.get("thread_ts"), str):
            state["thread_ts"] = data["thread_ts"]
        return state
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load state file %s: %s", state_file, e)
        return _SlackState()


def post_slack_message(
    message: str,
    color: str,
    state_file: Path,
    slack_token: str | None = None,
    slack_channel: str | None = None,
) -> bool:
    """Post a message to Slack in the existing build thread.

    Returns True on success, False on skip or error.
    """
    if not slack_channel:
        logger.warning("slack_channel is not set. Skipping Slack notification.")
        return False

    client = get_slack_client(slack_token)
    if client is None:
        return False

    try:
        from slack_sdk.errors import SlackApiError

        state = load_state(state_file)
        thread_ts = state.get("thread_ts")

        attachments = [
            {
                "color": color,
                "fallback": message,
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": message}}],
            }
        ]

        if thread_ts:
            client.chat_postMessage(
                channel=slack_channel,
                text=message,
                attachments=attachments,
                thread_ts=thread_ts,
            )
        else:
            client.chat_postMessage(
                channel=slack_channel,
                text=message,
                attachments=attachments,
            )
        logger.info("Posted Slack message: %s", message)
        return True
    except SlackApiError as e:
        logger.error("SlackApiError in post_slack_message: %s", e)
        return False
    except (OSError, ValueError) as e:
        logger.error("Error in post_slack_message: %s", e)
        return False
