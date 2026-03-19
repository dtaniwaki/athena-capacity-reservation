"""Tests for athena_capacity_reservation.slack."""

from athena_capacity_reservation.slack import get_slack_client, post_slack_message


def test_get_slack_client_returns_none_without_token() -> None:
    assert get_slack_client(None) is None


def test_post_slack_message_returns_false_without_channel() -> None:
    assert post_slack_message("msg", "#000000", slack_token="xoxb-test") is False


def test_post_slack_message_returns_false_without_token() -> None:
    assert post_slack_message("msg", "#000000", slack_channel="C123") is False
