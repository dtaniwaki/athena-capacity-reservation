"""CLI entry point for athena_capacity_reservation."""


import argparse
import logging
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

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


def _positive_int(value: str) -> int:
    try:
        i = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not an integer")
    if i <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {i}")
    return i


def _threshold_float(value: str) -> float:
    try:
        f = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number")
    if not (0 < f < 100):
        raise argparse.ArgumentTypeError(f"must be strictly between 0 and 100 (exclusive), got {f}")
    return f


def _build_settings(args: argparse.Namespace) -> Settings:
    """Build Settings from CLI args (non-None values override env vars)."""
    overrides: dict[str, Any] = {}
    # Fields that map directly from argparse dest to Settings field name
    _DIRECT_FIELDS = [
        "reservation_name",
        "workgroup_names",
        "dpus",
        "slack_channel",
        "min_dpus",
        "max_dpus",
        "scale_step_dpus",
        "scale_out_threshold",
        "scale_in_threshold",
        "monitor_interval",
        "cooldown_seconds",
        "queued_ticks_for_scale_out",
        "low_ticks_for_scale_in",
    ]
    for field_name in _DIRECT_FIELDS:
        val = getattr(args, field_name, None)
        if val is not None:
            overrides[field_name] = val

    # state-file and pid-file from CLI override env-based defaults
    state_file = getattr(args, "state_file", None)
    if state_file is not None:
        overrides["slack_state_file"] = Path(state_file)

    pid_file = getattr(args, "pid_file", None)
    if pid_file is not None:
        overrides["capacity_pid_file"] = Path(pid_file)

    return Settings(**overrides)


def _setup_logging(log_file: Path | None = None, log_level: str = "INFO") -> None:
    """Configure logging for the application."""
    handler: logging.Handler
    if log_file:
        handler = logging.FileHandler(log_file, mode="a")
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))


def _add_reservation_args(parser: argparse.ArgumentParser) -> None:
    """Add reservation-related CLI arguments to a subcommand parser."""
    parser.add_argument("--reservation-name", metavar="RESERVATION_NAME", help="Athena Capacity Reservation name")
    parser.add_argument("--slack-channel", metavar="CHANNEL_ID", help="Slack channel ID for notifications")


def _add_activate_args(parser: argparse.ArgumentParser) -> None:
    """Add activate-specific CLI arguments to a subcommand parser."""
    parser.add_argument(
        "--workgroup-names",
        metavar="NAMES",
        help="Comma-separated Athena workgroup names",
    )
    parser.add_argument("--dpus", type=_positive_int, metavar="N", help="DPU count for the capacity reservation")


def _add_state_file_arg(parser: argparse.ArgumentParser, default_path: str) -> None:
    parser.add_argument(
        "--state-file",
        default=None,
        help=f"Path to JSON file storing Slack notification state (default: {default_path})",
    )


def _add_pid_file_arg(parser: argparse.ArgumentParser, default_path: str) -> None:
    parser.add_argument(
        "--pid-file",
        default=None,
        help=f"Path to PID file (default: {default_path})",
    )


def _add_daemon_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Fork into background and return immediately (parent writes PID file). Requires --log-file.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="FILE",
        help="Path to log file. Required when using --daemon.",
    )


def _add_monitor_args(parser: argparse.ArgumentParser) -> None:
    """Add monitor-specific CLI arguments to a subcommand parser."""
    parser.add_argument("--min-dpus", type=_positive_int, metavar="N", help="Scale-in lower bound")
    parser.add_argument("--max-dpus", type=_positive_int, metavar="N", help="Scale-out upper bound")
    parser.add_argument(
        "--scale-step-dpus", type=_positive_int, metavar="N", help="DPU step per scale event (default: 8)"
    )
    parser.add_argument(
        "--scale-out-threshold",
        type=_threshold_float,
        metavar="PCT",
        help="Utilization %% to trigger scale-out (default: 80), must be in range (0, 100)",
    )
    parser.add_argument(
        "--scale-in-threshold",
        type=_threshold_float,
        metavar="PCT",
        help="Utilization %% to trigger scale-in (default: 30), must be in range (0, 100)",
    )
    parser.add_argument(
        "--monitor-interval", type=_positive_int, metavar="SEC", help="Poll interval in seconds (default: 60)"
    )
    parser.add_argument(
        "--cooldown-seconds", type=_positive_int, metavar="SEC", help="Min seconds between scale events (default: 300)"
    )
    parser.add_argument(
        "--queued-ticks-for-scale-out",
        type=_positive_int,
        metavar="N",
        help="Consecutive ticks with queued queries before scale-out (default: 2)",
    )
    parser.add_argument(
        "--low-ticks-for-scale-in",
        type=_positive_int,
        metavar="N",
        help="Consecutive ticks below scale-in threshold before scale-in (default: 2)",
    )


def main() -> None:
    """Main entry point for capacity command."""
    default_state_file = str(Settings.model_fields["slack_state_file"].default)
    default_pid_file = str(Settings.model_fields["capacity_pid_file"].default)

    parser = argparse.ArgumentParser(description="Manage Athena Capacity Reservation and autoscale monitor")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('athena-capacity-reservation')}",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- activate ---
    activate_parser = subparsers.add_parser("activate", help="Activate the Athena Capacity Reservation")
    _add_state_file_arg(activate_parser, default_state_file)
    _add_reservation_args(activate_parser)
    _add_activate_args(activate_parser)

    # --- deactivate ---
    deactivate_parser = subparsers.add_parser("deactivate", help="Deactivate the Athena Capacity Reservation")
    _add_state_file_arg(deactivate_parser, default_state_file)
    _add_reservation_args(deactivate_parser)

    # --- monitor (group) ---
    monitor_parser = subparsers.add_parser("monitor", help="Manage the autoscale monitor")
    monitor_subparsers = monitor_parser.add_subparsers(dest="monitor_command", required=True)

    # --- monitor start ---
    monitor_start_parser = monitor_subparsers.add_parser(
        "start", help="Run the autoscale monitor loop only (no activate/deactivate)"
    )
    _add_state_file_arg(monitor_start_parser, default_state_file)
    _add_pid_file_arg(monitor_start_parser, default_pid_file)
    _add_daemon_args(monitor_start_parser)
    _add_reservation_args(monitor_start_parser)
    _add_monitor_args(monitor_start_parser)

    # --- monitor stop ---
    monitor_stop_parser = monitor_subparsers.add_parser(
        "stop", help="Stop the background monitor via PID file (no deactivate)"
    )
    _add_pid_file_arg(monitor_stop_parser, default_pid_file)

    # --- start ---
    start_parser = subparsers.add_parser(
        "start", help="Activate reservation, then start the autoscale monitor (shortcut)"
    )
    _add_state_file_arg(start_parser, default_state_file)
    _add_pid_file_arg(start_parser, default_pid_file)
    _add_daemon_args(start_parser)
    _add_reservation_args(start_parser)
    _add_activate_args(start_parser)
    _add_monitor_args(start_parser)

    # --- stop ---
    stop_parser = subparsers.add_parser(
        "stop", help="Stop the background monitor, then deactivate the reservation (shortcut)"
    )
    _add_pid_file_arg(stop_parser, default_pid_file)
    _add_state_file_arg(stop_parser, default_state_file)
    _add_reservation_args(stop_parser)

    args = parser.parse_args()

    log_file = Path(args.log_file) if getattr(args, "log_file", None) else None

    if getattr(args, "daemon", False) and log_file is None:
        print(
            "Warning: --daemon specified without --log-file. "
            "Daemon process logs will be lost after detaching from the terminal.",
            file=sys.stderr,
        )

    _setup_logging(log_file, log_level=args.log_level)

    settings = _build_settings(args)

    try:
        if args.command == "activate":
            cmd_activate(settings)
        elif args.command == "deactivate":
            cmd_deactivate(settings)
        elif args.command == "monitor":
            if args.monitor_command == "start":
                cmd_monitor_start(settings, daemon=args.daemon, log_file=log_file)
            elif args.monitor_command == "stop":
                cmd_monitor_stop(settings.capacity_pid_file)
        elif args.command == "start":
            cmd_start(settings, daemon=args.daemon, log_file=log_file)
        elif args.command == "stop":
            cmd_stop(settings)
    except MonitorStopError as e:
        logging.getLogger(__name__).error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
