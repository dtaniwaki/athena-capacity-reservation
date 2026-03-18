"""Command functions for athena_capacity_reservation."""

from __future__ import annotations

import logging
import os
import signal
from pathlib import Path
from typing import TYPE_CHECKING

from athena_capacity_reservation.constants import (
    COLOR_FAILURE,
    COLOR_SUCCESS,
)
from athena_capacity_reservation.monitor import _daemonize, _run_monitor_loop
from athena_capacity_reservation.reservation import (
    _activate_capacity_reservation_direct,
    _deactivate_capacity_reservation_direct,
    _poll_until_active,
)
from athena_capacity_reservation.slack import post_slack_message

if TYPE_CHECKING:
    from athena_capacity_reservation.settings import Settings

logger = logging.getLogger(__name__)


class MonitorStopError(RuntimeError):
    """Raised when the monitor process cannot be stopped."""


def _post_slack(message: str, color: str, settings: Settings) -> bool:
    return post_slack_message(
        message,
        color,
        settings.slack_state_file,
        slack_token=settings.slack_token,
        slack_channel=settings.slack_channel,
    )


def _stop_monitor_process(pid_file: Path) -> bool:
    """Send SIGTERM to the monitor process via PID file.

    Returns True if the monitor was stopped (or PID file didn't exist).
    Raises MonitorStopError on failure.
    """
    if not pid_file.exists():
        logger.info("PID file %s not found; monitor may not be running.", pid_file)
        return True
    try:
        pid = int(pid_file.read_text().strip())
    except ValueError as e:
        raise MonitorStopError(f"Invalid PID in {pid_file}: {e}") from e
    try:
        os.kill(pid, signal.SIGTERM)
        logger.info("Sent SIGTERM to capacity monitor (pid=%d)", pid)
        pid_file.unlink(missing_ok=True)
        return True
    except ProcessLookupError as e:
        raise MonitorStopError(f"Capacity monitor process (pid={pid}) not found.") from e
    except OSError as e:
        raise MonitorStopError(f"Failed to send SIGTERM to capacity monitor (pid={pid}): {e}") from e


def cmd_activate(settings: Settings) -> None:
    """Activate Athena Capacity Reservation and poll until ACTIVE."""
    if not settings.reservation_name:
        raise RuntimeError("reservation_name is not set.")

    if not settings.workgroup_names:
        raise RuntimeError("workgroup_names is not set or empty.")

    if settings.dpus is None:
        raise RuntimeError("dpus is not set.")

    logger.info(
        "Activating Athena Capacity Reservation '%s' with %d DPUs...",
        settings.reservation_name,
        settings.dpus,
    )

    try:
        _activate_capacity_reservation_direct(settings.reservation_name, settings.workgroup_names, settings.dpus)
    except Exception as e:
        slack_msg = f"⚠️ Athena Capacity Reservation activation failed: {e}"
        logger.error(slack_msg)
        _post_slack(slack_msg, COLOR_FAILURE, settings)
        raise

    try:
        _poll_until_active(settings.reservation_name)
    except TimeoutError as e:
        slack_msg = f"⚠️ Athena Capacity Reservation activation timed out: {e}"
        logger.error(slack_msg)
        _post_slack(slack_msg, COLOR_FAILURE, settings)
        raise

    logger.info(
        "Athena Capacity Reservation '%s' is ACTIVE (%d DPUs).",
        settings.reservation_name,
        settings.dpus,
    )
    slack_msg = f"⚡ Athena Capacity Reservation activated ({settings.dpus} DPUs)"
    _post_slack(slack_msg, COLOR_SUCCESS, settings)


def cmd_deactivate(settings: Settings) -> None:
    """Deactivate Athena Capacity Reservation."""
    if not settings.reservation_name:
        raise RuntimeError("reservation_name is not set.")

    logger.info("Deactivating Athena Capacity Reservation '%s'...", settings.reservation_name)

    try:
        deactivate_result = _deactivate_capacity_reservation_direct(settings.reservation_name)
    except Exception as e:
        slack_msg = f"⚠️ Athena Capacity Reservation deactivation failed (manual cleanup may be required): {e}"
        logger.error(slack_msg)
        _post_slack(slack_msg, COLOR_FAILURE, settings)
        raise

    if deactivate_result == "update_pending_timeout":
        slack_msg = "⚠️ Athena Capacity Reservation deactivation skipped (UPDATE_PENDING timeout)"
        logger.warning(slack_msg)
        _post_slack(slack_msg, COLOR_FAILURE, settings)
        return

    logger.info("Athena Capacity Reservation '%s' deactivated.", settings.reservation_name)
    _post_slack("🔋 Athena Capacity Reservation deactivated", COLOR_SUCCESS, settings)


def cmd_monitor_start(settings: Settings, daemon: bool = False, log_file: Path | None = None) -> None:
    """Run the autoscale monitor loop only (no activate/deactivate)."""
    cfg = settings.build_monitor_config()
    if daemon:
        _daemonize(settings.capacity_pid_file, log_file)
    _run_monitor_loop(cfg)


def cmd_monitor_stop(pid_file: Path) -> None:
    """Stop the running capacity monitor by sending SIGTERM via PID file (no deactivate)."""
    _stop_monitor_process(pid_file)


def cmd_start(settings: Settings, daemon: bool = False, log_file: Path | None = None) -> None:
    """Activate the Athena Capacity Reservation, then run the autoscale monitor loop."""
    cfg = settings.build_monitor_config()

    cmd_activate(settings)
    if daemon:
        _daemonize(settings.capacity_pid_file, log_file)
    _run_monitor_loop(cfg)


def cmd_stop(settings: Settings) -> None:
    """Stop the background monitor, then deactivate the reservation."""
    monitor_stop_failed = False
    try:
        _stop_monitor_process(settings.capacity_pid_file)
    except MonitorStopError as e:
        logger.warning("Monitor stop failed: %s", e)
        monitor_stop_failed = True

    try:
        cmd_deactivate(settings)
    except Exception as e:
        if monitor_stop_failed:
            raise RuntimeError(f"Both monitor stop and deactivation failed. Deactivation error: {e}") from e
        raise
