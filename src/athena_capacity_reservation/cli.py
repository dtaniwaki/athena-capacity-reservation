"""CLI entry point for athena_capacity_reservation."""

import difflib
import functools
import logging
import sys
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path
from typing import Any, TypedDict, cast

import click


class CliContext(TypedDict):
    """Typed context object passed through Click's ctx.obj."""

    log_level: str

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


class SuggestGroup(click.Group):
    """click.Group subclass that suggests similar command names on typo."""

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str | None, click.Command | None, list[str]]:
        try:
            return super().resolve_command(ctx, args)
        except click.UsageError as e:
            cmd_name = args[0] if args else None
            if cmd_name is not None:
                matches = difflib.get_close_matches(cmd_name, self.list_commands(ctx), n=3, cutoff=0.6)
                if matches:
                    suggestion = ", ".join(matches)
                    e.message += f"\n\nDid you mean one of these?\n    {suggestion}"
            raise


POSITIVE_INT = click.IntRange(min=1)
THRESHOLD_FLOAT = click.FloatRange(min=0, max=100, min_open=True, max_open=True)


# ---------------------------------------------------------------------------
# Reusable option decorator bundles
# ---------------------------------------------------------------------------


def _compose(*decorators: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(func: Any) -> Any:
        for dec in reversed(decorators):
            func = dec(func)
        return func

    return wrapper


def reservation_options(func: Any) -> Any:
    @click.option("--reservation-name", default=None, metavar="NAME", help="Athena Capacity Reservation name")
    @click.option("--slack-channel", default=None, metavar="CHANNEL_ID", help="Slack channel ID for notifications")
    @click.option("--slack-thread-ts", default=None, metavar="TS", help="Slack thread timestamp to reply into")
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


def activate_options(func: Any) -> Any:
    @click.option("--workgroup-names", default=None, metavar="NAMES", help="Comma-separated Athena workgroup names")
    @click.option("--dpus", default=None, type=POSITIVE_INT, metavar="N", help="DPU count for the capacity reservation")
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


def pid_file_option(func: Any) -> Any:
    default_path = str(Settings.model_fields["capacity_pid_file"].default)

    @click.option(
        "--pid-file",
        default=None,
        metavar="FILE",
        help=f"Path to PID file (default: {default_path})",
    )
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


def daemon_options(func: Any) -> Any:
    @click.option(
        "--daemon",
        is_flag=True,
        default=False,
        help="Fork into background and return immediately. Requires --log-file.",
    )
    @click.option(
        "--log-file",
        default=None,
        metavar="FILE",
        help="Path to log file. Required when using --daemon.",
    )
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


def monitor_options(func: Any) -> Any:
    @click.option("--min-dpus", default=None, type=POSITIVE_INT, metavar="N", help="Scale-in lower bound")
    @click.option("--max-dpus", default=None, type=POSITIVE_INT, metavar="N", help="Scale-out upper bound")
    @click.option(
        "--scale-step-dpus", default=None, type=POSITIVE_INT, metavar="N", help="DPU step per scale event (default: 8)"
    )
    @click.option(
        "--scale-out-threshold",
        default=None,
        type=THRESHOLD_FLOAT,
        metavar="PCT",
        help="Utilization %% to trigger scale-out (default: 80)",
    )
    @click.option(
        "--scale-in-threshold",
        default=None,
        type=THRESHOLD_FLOAT,
        metavar="PCT",
        help="Utilization %% to trigger scale-in (default: 30)",
    )
    @click.option(
        "--monitor-interval",
        default=None,
        type=POSITIVE_INT,
        metavar="SEC",
        help="Poll interval in seconds (default: 60)",
    )
    @click.option(
        "--cooldown-seconds",
        default=None,
        type=POSITIVE_INT,
        metavar="SEC",
        help="Min seconds between scale events (default: 300)",
    )
    @click.option(
        "--queued-ticks-for-scale-out",
        default=None,
        type=POSITIVE_INT,
        metavar="N",
        help="Consecutive ticks with queued queries before scale-out (default: 2)",
    )
    @click.option(
        "--high-ticks-for-scale-out",
        default=None,
        type=POSITIVE_INT,
        metavar="N",
        help="Consecutive ticks with high utilization before scale-out (default: 5)",
    )
    @click.option(
        "--low-ticks-for-scale-in",
        default=None,
        type=POSITIVE_INT,
        metavar="N",
        help="Consecutive ticks below scale-in threshold before scale-in (default: 2)",
    )
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# Settings construction
# ---------------------------------------------------------------------------

_SETTINGS_FIELDS = [
    "reservation_name",
    "workgroup_names",
    "dpus",
    "slack_channel",
    "slack_thread_ts",
    "min_dpus",
    "max_dpus",
    "scale_step_dpus",
    "scale_out_threshold",
    "scale_in_threshold",
    "monitor_interval",
    "cooldown_seconds",
    "queued_ticks_for_scale_out",
    "high_ticks_for_scale_out",
    "low_ticks_for_scale_in",
]


def _build_settings(kwargs: dict[str, Any]) -> Settings:
    overrides: dict[str, Any] = {}
    for field_name in _SETTINGS_FIELDS:
        val = kwargs.get(field_name)
        if val is not None:
            overrides[field_name] = val

    pid_file = kwargs.get("pid_file")
    if pid_file is not None:
        overrides["capacity_pid_file"] = Path(pid_file)

    return Settings(**overrides)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _setup_logging(log_file: Path | None = None, log_level: str = "INFO") -> None:
    handler: logging.Handler
    if log_file:
        handler = logging.FileHandler(log_file, mode="a")
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group(cls=SuggestGroup)
@click.version_option(version=version("athena-capacity-reservation"), prog_name="athena-capacity-reservation")
@click.option(
    "--log-level",
    default="INFO",
    envvar="ATHENA_CR_LOG_LEVEL",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set the logging level (default: INFO)",
)
@click.pass_context
def main(ctx: click.Context, log_level: str) -> None:
    """Manage Athena Capacity Reservation and autoscale monitor."""
    ctx.ensure_object(dict)
    obj: CliContext = ctx.obj
    obj["log_level"] = log_level
    _setup_logging(log_level=log_level)


@main.command()
@reservation_options
@activate_options
@click.pass_context
def activate(ctx: click.Context, /, **kwargs: Any) -> None:
    """Activate the Athena Capacity Reservation."""
    settings = _build_settings(kwargs)
    cmd_activate(settings)


@main.command()
@reservation_options
@click.pass_context
def deactivate(ctx: click.Context, /, **kwargs: Any) -> None:
    """Deactivate the Athena Capacity Reservation."""
    settings = _build_settings(kwargs)
    cmd_deactivate(settings)


@main.group(cls=SuggestGroup)
def monitor() -> None:
    """Manage the autoscale monitor."""


@monitor.command("start")
@reservation_options
@pid_file_option
@daemon_options
@monitor_options
@click.pass_context
def monitor_start(ctx: click.Context, /, **kwargs: Any) -> None:
    """Run the autoscale monitor loop only (no activate/deactivate)."""
    daemon = kwargs.pop("daemon", False)
    log_file_str = kwargs.pop("log_file", None)
    log_file = Path(log_file_str) if log_file_str else None

    if daemon and log_file is None:
        click.echo(
            "Warning: --daemon specified without --log-file. "
            "Daemon process logs will be lost after detaching from the terminal.",
            err=True,
        )

    if log_file:
        _setup_logging(log_file, log_level=cast(CliContext, ctx.obj)["log_level"])

    settings = _build_settings(kwargs)
    cmd_monitor_start(settings, daemon=daemon, log_file=log_file)


@monitor.command("stop")
@pid_file_option
@click.pass_context
def monitor_stop(ctx: click.Context, /, **kwargs: Any) -> None:
    """Stop the background monitor via PID file (no deactivate)."""
    settings = _build_settings(kwargs)
    try:
        cmd_monitor_stop(settings.capacity_pid_file)
    except MonitorStopError as e:
        logging.getLogger(__name__).error("%s", e)
        ctx.exit(1)


@main.command()
@reservation_options
@activate_options
@pid_file_option
@daemon_options
@monitor_options
@click.pass_context
def start(ctx: click.Context, /, **kwargs: Any) -> None:
    """Activate reservation, then start the autoscale monitor (shortcut)."""
    daemon = kwargs.pop("daemon", False)
    log_file_str = kwargs.pop("log_file", None)
    log_file = Path(log_file_str) if log_file_str else None

    if daemon and log_file is None:
        click.echo(
            "Warning: --daemon specified without --log-file. "
            "Daemon process logs will be lost after detaching from the terminal.",
            err=True,
        )

    if log_file:
        _setup_logging(log_file, log_level=cast(CliContext, ctx.obj)["log_level"])

    settings = _build_settings(kwargs)
    cmd_start(settings, daemon=daemon, log_file=log_file)


@main.command()
@reservation_options
@pid_file_option
@click.pass_context
def stop(ctx: click.Context, /, **kwargs: Any) -> None:
    """Stop the background monitor, then deactivate the reservation (shortcut)."""
    settings = _build_settings(kwargs)
    try:
        cmd_stop(settings)
    except MonitorStopError as e:
        logging.getLogger(__name__).error("%s", e)
        ctx.exit(1)


if __name__ == "__main__":
    main()
