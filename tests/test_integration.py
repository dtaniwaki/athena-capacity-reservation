"""Integration tests for athena_capacity_reservation."""

import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from athena_capacity_reservation import cmd_start, cmd_stop
from athena_capacity_reservation.monitor import _MonitorConfig, _run_monitor_loop
from athena_capacity_reservation.settings import Settings

# ---------------------------------------------------------------------------
# cmd_start integration scenarios
# ---------------------------------------------------------------------------


def test_cmd_start_calls_build_monitor_config_then_activate_then_monitor(tmp_path: Path) -> None:
    """cmd_start() calls build_monitor_config() -> cmd_activate() -> _run_monitor_loop() in order."""
    s = Settings(
        reservation_name="my-res",
        dpus=8,
        workgroup_names=["wg"],
        slack_state_file=tmp_path / "state.json",
        capacity_pid_file=tmp_path / "monitor.pid",
    )
    call_order: list[str] = []

    def fake_activate(settings: object) -> None:
        call_order.append("cmd_activate")

    def fake_run_loop(cfg: object) -> None:
        call_order.append("run_monitor_loop")

    with (
        patch("athena_capacity_reservation.commands.cmd_activate", side_effect=fake_activate),
        patch("athena_capacity_reservation.commands._run_monitor_loop", side_effect=fake_run_loop),
        patch("athena_capacity_reservation.commands._daemonize"),
    ):
        cmd_start(s, daemon=False)

    assert call_order == ["cmd_activate", "run_monitor_loop"]


def test_cmd_stop_sends_sigterm_then_deactivates(tmp_path: Path) -> None:
    """cmd_stop() sends SIGTERM to the monitor process, then calls cmd_deactivate()."""
    import signal

    pid_file = tmp_path / "monitor.pid"
    state_file = tmp_path / "state.json"
    pid_file.write_text("12345")

    s = Settings(
        reservation_name="my-res",
        dpus=8,
        slack_state_file=state_file,
        capacity_pid_file=pid_file,
    )

    with (
        patch("os.kill") as mock_kill,
        patch("athena_capacity_reservation.commands.cmd_deactivate") as mock_deactivate,
    ):
        cmd_stop(s)

    mock_kill.assert_called_once_with(12345, signal.SIGTERM)
    mock_deactivate.assert_called_once_with(s)
    assert not pid_file.exists()


def test_cmd_start_raises_without_reservation_name_before_activate(tmp_path: Path) -> None:
    """cmd_start() raises RuntimeError without activating when reservation_name is not set."""
    s = Settings(
        dpus=8,
        slack_state_file=tmp_path / "state.json",
        capacity_pid_file=tmp_path / "monitor.pid",
    )

    with (
        patch("athena_capacity_reservation.commands.cmd_activate") as mock_activate,
    ):
        with pytest.raises(RuntimeError, match="reservation_name"):
            cmd_start(s)

    mock_activate.assert_not_called()


# ---------------------------------------------------------------------------
# Monitor lifecycle simulation (thread + stop_event instead of fork + SIGTERM)
# ---------------------------------------------------------------------------


def test_monitor_loop_starts_and_stops_via_stop_event(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    cfg = _MonitorConfig(
        reservation_name="res",
        min_dpus=8,
        max_dpus=8,
        monitor_interval_seconds=60,
        state_file=state_file,
    )
    stop_event = threading.Event()

    with (
        patch("athena_capacity_reservation.monitor.boto3"),
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=None),
    ):
        thread = threading.Thread(
            target=_run_monitor_loop,
            args=(cfg,),
            kwargs={"stop_event": stop_event},
            daemon=True,
        )
        thread.start()

        stop_event.set()
        thread.join(timeout=2)

    assert not thread.is_alive()


def test_monitor_loop_runs_check_and_scale_then_stops(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    cfg = _MonitorConfig(
        reservation_name="res",
        min_dpus=8,
        max_dpus=8,
        monitor_interval_seconds=0,
        state_file=state_file,
    )
    stop_event = threading.Event()
    tick_count = 0

    def fake_check_and_scale(c: object, last: float, **kw: object) -> tuple[float, int, int]:
        nonlocal tick_count
        tick_count += 1
        stop_event.set()
        return last, 0, 0

    with (
        patch("athena_capacity_reservation.monitor.boto3"),
        patch("athena_capacity_reservation.monitor._check_and_scale", side_effect=fake_check_and_scale),
    ):
        thread = threading.Thread(
            target=_run_monitor_loop,
            args=(cfg,),
            kwargs={"stop_event": stop_event},
            daemon=True,
        )
        thread.start()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert tick_count == 1
