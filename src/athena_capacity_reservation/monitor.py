"""Monitor loop, autoscaling, and daemon for athena_capacity_reservation."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, Literal, NamedTuple

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_cloudwatch import CloudWatchClient

from athena_capacity_reservation.slack import post_slack_message

logger = logging.getLogger(__name__)


class ConsumedStat(StrEnum):
    """CloudWatch Stat values supported for DPUConsumed metric."""

    AVERAGE = "average"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    P50 = "p50"
    P90 = "p90"
    P95 = "p95"
    P99 = "p99"

    @property
    def cloudwatch_value(self) -> str:
        """Return the value as CloudWatch API expects it (capitalized for standard stats)."""
        standard = {"average": "Average", "minimum": "Minimum", "maximum": "Maximum"}
        return standard.get(self.value, self.value)


class DpuMetrics(NamedTuple):
    """DPU utilization and allocation from CloudWatch."""

    utilization_percent: float
    allocated_dpus: float


class ScaleResult(NamedTuple):
    """Return value of _scale_capacity_reservation."""

    action: Literal["scaled", "skipped"]
    from_dpus: int
    to_dpus: int


class ScaleCheckResult(NamedTuple):
    """Return value of _check_and_scale."""

    last_scale_time: float
    queued_ticks: int
    low_ticks: int
    high_ticks: int


@dataclass
class _MonitorConfig:
    """Runtime configuration for the autoscale monitor loop."""

    reservation_name: str
    min_dpus: int
    max_dpus: int
    scale_out_threshold: float = 70.0
    scale_in_threshold: float = 50.0
    scale_step_dpus: int = 8
    monitor_interval_seconds: int = 60
    cooldown_seconds: int = 180
    workgroup_names: list[str] = field(default_factory=list)
    min_queued_ticks: int = 2
    min_high_ticks: int = 3
    min_low_ticks: int = 2
    consumed_stat: ConsumedStat = ConsumedStat.P90
    slack_token: str | None = None
    slack_channel: str | None = None
    slack_thread_ts: str | None = None



def _get_dpu_metrics(
    reservation_name: str,
    lookback_seconds: int = 300,
    *,
    consumed_stat: ConsumedStat = ConsumedStat.P90,
    cw_client: CloudWatchClient | None = None,
) -> DpuMetrics | None:
    """Return (utilization_percent, allocated_dpus) from CloudWatch, or None if no data.

    Queries DPUConsumed and DPUAllocated separately (no metric math) and computes
    utilization in Python.  This avoids CloudWatch metric math treating missing
    consumed data as 0, which would produce false 0% readings when DPUConsumed
    has a publishing delay of 1-2 minutes.

    lookback_seconds defaults to 300s (5 min) to tolerate CloudWatch publishing
    delays.  The most recent data point for each metric is used.
    """
    try:
        cw = cw_client or boto3.client("cloudwatch")
        now = datetime.now(UTC)
        response = cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "allocated",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Athena",
                            "MetricName": "DPUAllocated",
                            "Dimensions": [{"Name": "Capacity Reservation", "Value": reservation_name}],
                        },
                        "Period": 60,
                        "Stat": "Maximum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "consumed",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Athena",
                            "MetricName": "DPUConsumed",
                            "Dimensions": [{"Name": "Capacity Reservation", "Value": reservation_name}],
                        },
                        "Period": 60,
                        "Stat": consumed_stat.cloudwatch_value,
                    },
                    "ReturnData": True,
                },
            ],
            StartTime=now - timedelta(seconds=lookback_seconds),
            EndTime=now,
            ScanBy="TimestampDescending",
        )
        results = {r["Id"]: r for r in response.get("MetricDataResults", [])}
        allocated_values = results.get("allocated", {}).get("Values", [])
        consumed_values = results.get("consumed", {}).get("Values", [])
        if not allocated_values:
            logger.warning("No DPUAllocated data in CloudWatch window (possible metric ingestion lag)")
            return None
        if not consumed_values:
            logger.warning(
                "No DPUConsumed data in CloudWatch window (possible publishing delay), "
                "skipping scaling decision",
            )
            return None
        allocated = allocated_values[0]
        consumed = consumed_values[0]
        utilization = (consumed / allocated * 100) if allocated > 0 else 0.0
        return DpuMetrics(utilization, allocated)
    except ClientError as e:
        logger.warning("CloudWatch API error getting DPU metrics: %s", e)
        return None


def _scale_capacity_reservation(
    reservation_name: str,
    dpu_delta: int,
    min_dpus: int,
    max_dpus: int,
    *,
    athena_client: AthenaClient | None = None,
) -> ScaleResult:
    """Scale an Athena Capacity Reservation by dpu_delta DPUs, clamped to [min_dpus, max_dpus].

    Returns ScaleResult(action, from_dpus, to_dpus) where action is "scaled" or "skipped".
    Raises ClientError on AWS API failure.
    """
    athena = athena_client or boto3.client("athena")
    response = athena.get_capacity_reservation(Name=reservation_name)
    status = response["CapacityReservation"]["Status"]
    current_dpus = int(response["CapacityReservation"]["TargetDpus"])
    if status == "UPDATE_PENDING":
        logger.warning("Reservation '%s' is UPDATE_PENDING, skipping scale", reservation_name)
        return ScaleResult("skipped", current_dpus, current_dpus)
    target_dpus = max(min_dpus, min(max_dpus, current_dpus + dpu_delta))
    if target_dpus == current_dpus:
        return ScaleResult("skipped", current_dpus, current_dpus)
    athena.update_capacity_reservation(Name=reservation_name, TargetDpus=target_dpus)
    return ScaleResult("scaled", current_dpus, target_dpus)


def _check_and_scale(
    cfg: _MonitorConfig,
    last_scale_time: float,
    queued_ticks: int = 0,
    low_ticks: int = 0,
    high_ticks: int = 0,
    *,
    athena_client: AthenaClient | None = None,
    cw_client: CloudWatchClient | None = None,
) -> ScaleCheckResult:
    """Check utilization and scale if thresholds are crossed.

    Returns ScaleCheckResult(last_scale_time, queued_ticks, low_ticks, high_ticks) where:
    - queued_ticks counts consecutive ticks with utilization >= 100% (all DPUs consumed,
      new queries likely queued). Used as an accelerated scale-out trigger after
      cfg.min_queued_ticks consecutive ticks.
    - low_ticks counts consecutive ticks with utilization <= cfg.scale_in_threshold.
      Scale-in triggers only after cfg.min_low_ticks consecutive ticks.
    - high_ticks counts consecutive ticks with utilization >= cfg.scale_out_threshold.
      Used as a sustained scale-out trigger after cfg.min_high_ticks consecutive ticks.

    Scale-out has two paths:
    - Sustained path: utilization >= threshold for cfg.min_high_ticks consecutive ticks.
    - Accelerated path: utilization >= 100% for cfg.min_queued_ticks consecutive ticks,
      bypassing the high_ticks requirement.
    """
    lookback = max(300, cfg.monitor_interval_seconds * 5)
    metrics = _get_dpu_metrics(
        cfg.reservation_name, lookback_seconds=lookback, consumed_stat=cfg.consumed_stat, cw_client=cw_client
    )
    if metrics is None:
        logger.debug("No DPU metrics data available yet, skipping")
        return ScaleCheckResult(last_scale_time, queued_ticks, low_ticks, high_ticks)

    utilization, allocated_dpus = metrics
    if allocated_dpus == 0:
        # Reservation not yet allocating DPUs (e.g. still transitioning to ACTIVE).
        # Utilization formula returns 0 in this case, which would falsely trigger scale-in.
        logger.debug("Allocated DPUs is 0 (reservation not yet active), skipping scale check")
        return ScaleCheckResult(last_scale_time, queued_ticks, low_ticks, high_ticks)
    logger.info("DPU utilization: %.1f%%, allocated: %.0f", utilization, allocated_dpus)

    in_cooldown = (time.time() - last_scale_time) < cfg.cooldown_seconds
    if in_cooldown:
        remaining = int(cfg.cooldown_seconds - (time.time() - last_scale_time))
        logger.debug("In cooldown (%ds remaining), skipping scale check", remaining)
        # Preserve tick counters across cooldown so that consecutive-tick
        # counters are not reset; scaling triggers immediately after cooldown expires
        # if the condition was already sustained before the previous scale event.
        return ScaleCheckResult(last_scale_time, queued_ticks, low_ticks, high_ticks)

    if utilization >= cfg.scale_out_threshold:
        low_ticks = 0
        new_high_ticks = high_ticks + 1
        new_queued_ticks = 0
        logger.info(
            "High utilization (consecutive ticks: %d/%d)",
            new_high_ticks,
            cfg.min_high_ticks,
        )

        # Accelerated path: infer queued queries from utilization >= 100%.
        # When consumed DPUs equal allocated DPUs, new queries are likely queued.
        # This replaces the previous Athena API-based check which caused
        # ThrottlingException due to exhaustive ListQueryExecutions pagination.
        if utilization >= 100.0:
            new_queued_ticks = queued_ticks + 1
            logger.info(
                "Queued queries likely (utilization >= 100%%, consecutive ticks: %d/%d)",
                new_queued_ticks,
                cfg.min_queued_ticks,
            )
        else:
            logger.info("No queued queries (utilization < 100%%)")

        # Either path met → scale out; otherwise defer.
        sustained = new_high_ticks >= cfg.min_high_ticks
        accelerated = new_queued_ticks >= cfg.min_queued_ticks
        if not sustained and not accelerated:
            logger.info(
                "Scale-out deferred: waiting for sustained high utilization (%d/%d ticks)"
                + (" or queued queries (%d/%d ticks)" if new_queued_ticks else ""),
                new_high_ticks,
                cfg.min_high_ticks,
                *([new_queued_ticks, cfg.min_queued_ticks] if new_queued_ticks else []),
            )
            return ScaleCheckResult(last_scale_time, new_queued_ticks, 0, new_high_ticks)

        dpu_delta = cfg.scale_step_dpus
    elif utilization <= cfg.scale_in_threshold:
        queued_ticks = 0
        high_ticks = 0
        new_low_ticks = low_ticks + 1
        logger.info(
            "Low utilization (consecutive ticks: %d/%d)",
            new_low_ticks,
            cfg.min_low_ticks,
        )
        if new_low_ticks < cfg.min_low_ticks:
            logger.info(
                "Scale-in deferred: waiting for sustained low utilization (%d/%d ticks)",
                new_low_ticks,
                cfg.min_low_ticks,
            )
            return ScaleCheckResult(last_scale_time, 0, new_low_ticks, 0)
        # No queued query check needed: utilization <= scale_in_threshold
        # implies consumed < allocated, so no queries are waiting for DPUs.
        dpu_delta = -cfg.scale_step_dpus
    else:
        logger.debug("Utilization within thresholds, skipping scale check")
        return ScaleCheckResult(last_scale_time, 0, 0, 0)

    action, from_dpus, to_dpus = _scale_capacity_reservation(
        cfg.reservation_name,
        dpu_delta,
        cfg.min_dpus,
        cfg.max_dpus,
        athena_client=athena_client,
    )
    if action != "scaled":
        logger.info("Scale no-op (clamped at %d DPU)", from_dpus)
        return ScaleCheckResult(last_scale_time, 0, 0, 0)
    if to_dpus > from_dpus:
        scale_msg = f"📈 Athena DPU scale-out: {from_dpus}→{to_dpus}"
    else:
        scale_msg = f"📉 Athena DPU scale-in: {from_dpus}→{to_dpus}"
    logger.info("%s", scale_msg)
    post_slack_message(
        scale_msg,
        slack_token=cfg.slack_token,
        slack_channel=cfg.slack_channel,
        slack_thread_ts=cfg.slack_thread_ts,
    )
    return ScaleCheckResult(time.time(), 0, 0, 0)


def _daemonize(pid_file: Path, log_file: Path | None = None) -> None:
    """Fork into background. Parent writes child PID to pid_file and exits immediately.

    Requires POSIX (os.fork). Not supported on Windows.
    """
    pid = os.fork()
    if pid > 0:
        # Parent: write child PID and exit so the caller returns immediately.
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(pid))
        sys.exit(0)
    # Child: detach from terminal.
    os.setsid()
    # Redirect stdin/stdout/stderr to /dev/null to avoid errors from detached terminal.
    # Use raw file descriptors 0/1/2 instead of sys.stdin.fileno() etc., because
    # sys.stdin may be replaced by test frameworks (e.g. pytest) with pseudofiles.
    devnull_r = os.open(os.devnull, os.O_RDONLY)
    devnull_w = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_r, 0)  # stdin
    os.dup2(devnull_w, 1)  # stdout
    os.dup2(devnull_w, 2)  # stderr
    os.close(devnull_r)
    os.close(devnull_w)
    # child: reconfigure logging to file if specified
    if log_file:
        # replace all handlers with file handler
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        fh = logging.FileHandler(log_file, mode="a")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        root.addHandler(fh)


def _run_monitor_loop(cfg: _MonitorConfig, stop_event: threading.Event | None = None) -> None:
    """Run the autoscale monitor loop until the stop_event is set.

    stop_event is set by SIGTERM/SIGINT handlers when running in the main thread
    (production/fork), or fired directly by the caller in tests (thread simulation).

    When stop_event is None, a new Event is created internally.
    Signal handlers are registered only when running in the main thread.
    """
    if stop_event is None:
        stop_event = threading.Event()

    def _handle_signal(sig: int, frame: FrameType | None) -> None:
        stop_event.set()

    # Signal handlers can only be registered from the main thread.
    # In tests, _run_monitor_loop runs in a worker thread and stop_event is
    # fired directly, so signal registration is skipped.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

    scale_out_parts = [f"utilization × {cfg.min_high_ticks} ticks"]
    scale_out_parts.append(f"queued (utilization >= 100%%) × {cfg.min_queued_ticks} ticks")
    scale_out_gate = ", scale-out gate: " + ", ".join(scale_out_parts)
    logger.info(
        "Capacity monitor started for '%s' "
        "(min=%d, max=%d, step=%d, "
        "interval=%ds, cooldown=%ds, "
        "scale-in gate: %d ticks%s)",
        cfg.reservation_name,
        cfg.min_dpus,
        cfg.max_dpus,
        cfg.scale_step_dpus,
        cfg.monitor_interval_seconds,
        cfg.cooldown_seconds,
        cfg.min_low_ticks,
        scale_out_gate,
    )

    athena_client = boto3.client("athena")
    cw_client = boto3.client("cloudwatch")

    # Initialize last_scale_time to now to prevent immediate scale-in on startup:
    # utilization is 0% before any queries begin, which would otherwise trigger scale-in.
    last_scale_time = time.time()
    queued_ticks = 0
    low_ticks = 0
    high_ticks = 0
    while not stop_event.is_set():
        # wait() returns True when the event is set (stopped), False on timeout (next tick).
        # This allows immediate response to stop_event without waiting the full interval.
        if stop_event.wait(timeout=cfg.monitor_interval_seconds):
            break
        try:
            result = _check_and_scale(
                cfg,
                last_scale_time,
                queued_ticks=queued_ticks,
                low_ticks=low_ticks,
                high_ticks=high_ticks,
                athena_client=athena_client,
                cw_client=cw_client,
            )
            last_scale_time = result.last_scale_time
            queued_ticks = result.queued_ticks
            low_ticks = result.low_ticks
            high_ticks = result.high_ticks
        except Exception as e:
            logger.error("Error in capacity monitor tick: %s", e)
            # Do not reset last_scale_time on error: resetting the cooldown timer
            # during API failures would permanently suppress scaling until the next
            # successful tick after the cooldown period.
            # Reset tick counters to avoid scaling based on stale pre-error state:
            # an uncertain state could lead to scale-out/in on false premises.
            queued_ticks = 0
            low_ticks = 0
            high_ticks = 0

    logger.info("Capacity monitor stopped")
