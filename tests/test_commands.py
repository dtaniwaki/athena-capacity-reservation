"""Tests for athena_capacity_reservation.commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from helpers import _settings

from athena_capacity_reservation.commands import (
    MonitorStopError,
    cmd_activate,
    cmd_deactivate,
    cmd_monitor_start,
    cmd_monitor_stop,
    cmd_start,
    cmd_stop,
)
from athena_capacity_reservation.settings import Settings

# ---------------------------------------------------------------------------
# cmd_activate
# ---------------------------------------------------------------------------


@patch("athena_capacity_reservation.commands._activate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_activate_direct_failure_raises(
    mock_slack: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.side_effect = RuntimeError("API error")
    s = _settings()

    with pytest.raises(RuntimeError, match="API error"):
        cmd_activate(s)

    mock_slack.assert_called_once()
    assert "\u26a0\ufe0f" in mock_slack.call_args[0][0]


def test_cmd_activate_errors_when_no_reservation_name() -> None:
    s = _settings(reservation_name=None)
    with pytest.raises(RuntimeError, match="reservation_name"):
        cmd_activate(s)


def test_cmd_activate_errors_when_no_dpus() -> None:
    s = _settings(dpus=None, min_dpus=None, max_dpus=None)
    with pytest.raises(RuntimeError, match="dpus is not set"):
        cmd_activate(s)


def test_cmd_activate_errors_when_no_workgroup_names() -> None:
    s = _settings(workgroup_names=[])
    with pytest.raises(RuntimeError, match="workgroup_names"):
        cmd_activate(s)


@patch("athena_capacity_reservation.commands._activate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands._poll_until_active")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_activate_direct_success_lambda_not_called(
    mock_slack: MagicMock,
    mock_poll: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.return_value = None
    mock_poll.return_value = None
    s = _settings(dpus=4)

    cmd_activate(s)

    mock_direct.assert_called_once_with("my-reservation", ["my-workgroup"], 4)
    mock_poll.assert_called_once_with("my-reservation")
    mock_slack.assert_called_once()
    assert "\u26a1" in mock_slack.call_args[0][0]


@patch("athena_capacity_reservation.commands._activate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands._poll_until_active")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_activate_timeout_sends_slack_and_raises(
    mock_slack: MagicMock,
    mock_poll: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.return_value = None
    mock_poll.side_effect = TimeoutError("Timed out")
    s = _settings(dpus=4)

    with pytest.raises(TimeoutError):
        cmd_activate(s)

    mock_slack.assert_called_once()
    assert "\u26a0\ufe0f" in mock_slack.call_args[0][0]
    assert "timed out" in mock_slack.call_args[0][0].lower()


# ---------------------------------------------------------------------------
# cmd_deactivate
# ---------------------------------------------------------------------------


@patch("athena_capacity_reservation.commands._deactivate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_deactivate_direct_success_lambda_not_called(
    mock_slack: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.return_value = "cancelled"
    s = _settings()

    cmd_deactivate(s)

    mock_direct.assert_called_once_with("my-reservation")
    mock_slack.assert_called_once()
    assert "\U0001f50b" in mock_slack.call_args[0][0]


@patch("athena_capacity_reservation.commands._deactivate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_deactivate_direct_failure_raises(
    mock_slack: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.side_effect = RuntimeError("API error")
    s = _settings()

    with pytest.raises(RuntimeError, match="API error"):
        cmd_deactivate(s)

    mock_slack.assert_called_once()
    assert "\u26a0\ufe0f" in mock_slack.call_args[0][0]


@patch("athena_capacity_reservation.commands._deactivate_capacity_reservation_direct")
@patch("athena_capacity_reservation.commands.post_slack_message")
def test_cmd_deactivate_update_pending_timeout_skips(
    mock_slack: MagicMock,
    mock_direct: MagicMock,
) -> None:
    mock_direct.return_value = "update_pending_timeout"
    s = _settings()

    cmd_deactivate(s)

    mock_slack.assert_called_once()
    msg_text = mock_slack.call_args[0][0]
    assert "\u26a0\ufe0f" in msg_text
    assert "UPDATE_PENDING" in msg_text


def test_cmd_deactivate_errors_when_no_reservation_name() -> None:
    s = _settings(reservation_name=None)
    with pytest.raises(RuntimeError, match="reservation_name"):
        cmd_deactivate(s)


# ---------------------------------------------------------------------------
# cmd_monitor_start
# ---------------------------------------------------------------------------


def test_cmd_monitor_start_errors_if_reservation_name_missing() -> None:
    s = Settings(dpus=8)
    with pytest.raises(RuntimeError, match="reservation_name"):
        cmd_monitor_start(s)


def test_cmd_monitor_start_derives_dpus_from_defaults(tmp_path: Path) -> None:
    s = Settings(
        reservation_name="my-res",
        dpus=4,
        max_dpus=16,
        capacity_pid_file=tmp_path / "monitor.pid",
    )
    with patch("athena_capacity_reservation.commands._run_monitor_loop") as mock_loop:
        cmd_monitor_start(s)
    mock_loop.assert_called_once()


# ---------------------------------------------------------------------------
# cmd_monitor_stop
# ---------------------------------------------------------------------------


def test_cmd_monitor_stop_sends_sigterm_no_deactivate(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("12345")
    with (
        patch("os.kill") as mock_kill,
        patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate,
    ):
        import signal

        cmd_monitor_stop(pid_file)
        mock_kill.assert_called_once_with(12345, signal.SIGTERM)
        mock_deactivate.assert_not_called()
    assert not pid_file.exists()
    captured = capsys.readouterr()
    assert "12345" in captured.err


def test_cmd_monitor_stop_no_pid_file_is_noop(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    with patch("os.kill") as mock_kill:
        cmd_monitor_stop(pid_file)
        mock_kill.assert_not_called()


def test_cmd_monitor_stop_process_already_gone(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("99999")
    with (
        patch("os.kill", side_effect=ProcessLookupError),
        pytest.raises(MonitorStopError, match="not found"),
    ):
        cmd_monitor_stop(pid_file)


def test_cmd_monitor_stop_invalid_pid_file(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("not-a-pid")
    with pytest.raises(MonitorStopError, match="Invalid PID"):
        cmd_monitor_stop(pid_file)


# ---------------------------------------------------------------------------
# cmd_start
# ---------------------------------------------------------------------------


def test_cmd_start_errors_if_reservation_name_missing(tmp_path: Path) -> None:
    s = Settings(dpus=8, capacity_pid_file=tmp_path / "monitor.pid")
    with (
        patch("athena_capacity_reservation.commands.cmd_activate") as mock_activate,
    ):
        with pytest.raises(RuntimeError, match="reservation_name"):
            cmd_start(s)
    mock_activate.assert_not_called()


def test_cmd_start_derives_dpus_from_defaults(tmp_path: Path) -> None:
    s = Settings(
        reservation_name="my-res",
        dpus=4,
        max_dpus=16,
        capacity_pid_file=tmp_path / "monitor.pid",
    )
    with (
        patch("athena_capacity_reservation.commands.cmd_activate") as mock_activate,
        patch("athena_capacity_reservation.commands._run_monitor_loop") as mock_loop,
    ):
        cmd_start(s)
    mock_activate.assert_called_once_with(s)
    mock_loop.assert_called_once()


def test_cmd_start_calls_activate_then_monitors_without_daemon(tmp_path: Path) -> None:
    s = _settings(capacity_pid_file=tmp_path / "monitor.pid")
    with (
        patch("athena_capacity_reservation.commands.cmd_activate") as mock_activate,
        patch("athena_capacity_reservation.commands._run_monitor_loop") as mock_monitor,
        patch("athena_capacity_reservation.commands._daemonize") as mock_daemonize,
    ):
        cmd_start(s, daemon=False)

    mock_activate.assert_called_once_with(s)
    mock_daemonize.assert_not_called()
    mock_monitor.assert_called_once()


def test_cmd_start_calls_activate_then_forks_and_monitors_with_daemon(tmp_path: Path) -> None:
    s = _settings(capacity_pid_file=tmp_path / "monitor.pid")
    with (
        patch("athena_capacity_reservation.commands.cmd_activate") as mock_activate,
        patch("athena_capacity_reservation.commands._run_monitor_loop") as mock_monitor,
        patch("athena_capacity_reservation.commands._daemonize") as mock_daemonize,
    ):
        cmd_start(s, daemon=True)

    mock_activate.assert_called_once_with(s)
    mock_daemonize.assert_called_once_with(s.capacity_pid_file, None)
    mock_monitor.assert_called_once()


# ---------------------------------------------------------------------------
# cmd_stop
# ---------------------------------------------------------------------------


def test_cmd_stop_sends_sigterm_then_deactivates(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("12345")
    s = _settings(capacity_pid_file=pid_file)
    with (
        patch("os.kill") as mock_kill,
        patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate,
    ):
        import signal

        cmd_stop(s)
        mock_kill.assert_called_once_with(12345, signal.SIGTERM)
        mock_deactivate.assert_called_once_with(s)
    assert not pid_file.exists()
    captured = capsys.readouterr()
    assert "12345" in captured.err


def test_cmd_stop_no_pid_file_still_deactivates(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    s = _settings(capacity_pid_file=pid_file)
    with patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate:
        cmd_stop(s)
        mock_deactivate.assert_called_once_with(s)


def test_cmd_stop_process_already_gone_still_deactivates(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("99999")
    s = _settings(capacity_pid_file=pid_file)
    with (
        patch("os.kill", side_effect=ProcessLookupError),
        patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate,
    ):
        cmd_stop(s)
    mock_deactivate.assert_called_once_with(s)


def test_cmd_stop_process_already_gone_deactivate_fails(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("99999")
    s = _settings(capacity_pid_file=pid_file)
    with (
        patch("os.kill", side_effect=ProcessLookupError),
        patch("athena_capacity_reservation.commands.cmd_deactivate", side_effect=RuntimeError("fail")),
        pytest.raises(RuntimeError, match="Both monitor stop and deactivation failed"),
    ):
        cmd_stop(s)


def test_cmd_stop_kill_oserror_still_deactivates(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("99999")
    s = _settings(capacity_pid_file=pid_file)
    with (
        patch("os.kill", side_effect=OSError("permission denied")),
        patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate,
    ):
        cmd_stop(s)
    mock_deactivate.assert_called_once_with(s)


def test_cmd_stop_invalid_pid_file_still_deactivates(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("not-a-pid")
    s = _settings(capacity_pid_file=pid_file)
    with patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate:
        cmd_stop(s)
    mock_deactivate.assert_called_once_with(s)


def test_cmd_stop_invalid_pid_file_deactivate_also_fails(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    pid_file.write_text("not-a-pid")
    s = _settings(capacity_pid_file=pid_file)
    with (
        patch("athena_capacity_reservation.commands.cmd_deactivate", side_effect=RuntimeError("fail")),
        pytest.raises(RuntimeError, match="Both monitor stop and deactivation failed"),
    ):
        cmd_stop(s)
