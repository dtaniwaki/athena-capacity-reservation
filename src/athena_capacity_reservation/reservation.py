"""Athena API operations for athena_capacity_reservation."""

import logging
import time
from typing import Literal

import boto3
from botocore.exceptions import ClientError

from athena_capacity_reservation.constants import (
    POLL_INTERVAL_SECONDS,
    POLL_MAX_CONSECUTIVE_ERRORS,
    POLL_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

_DEACTIVATE_DIRECT_POLL_INTERVAL_SECONDS = 30
_DEACTIVATE_DIRECT_POLL_MAX_ATTEMPTS = 6
_CANCELLED_DELETE_POLL_INTERVAL_SECONDS = 5
_CANCELLED_DELETE_POLL_MAX_ATTEMPTS = 12  # up to 60 seconds total


def _activate_capacity_reservation_direct(
    reservation_name: str,
    workgroup_names: list[str],
    target_dpus: int,
) -> None:
    """Activate the Athena Capacity Reservation directly via Athena API.

    Mirrors Lambda handle_activate logic:
    - CANCELLED: delete then create new
    - None (not found): create + assign workgroups
    - UPDATE_PENDING: no-op (another update in progress)
    - ACTIVE: update DPUs only if current < target (respect autoscaler)
    - CANCELLING: raise RuntimeError

    Uses Config(parameter_validation=False) to handle old botocore enforcing min 24 DPUs
    for CreateCapacityReservation.

    Raises ClientError or RuntimeError on failure.
    """
    from botocore.config import Config

    athena = boto3.client("athena", config=Config(parameter_validation=False))

    try:
        response = athena.get_capacity_reservation(Name=reservation_name)
        status: str | None = response["CapacityReservation"]["Status"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidRequestException":
            status = None
        else:
            raise

    if status == "CANCELLED":
        logger.info("Reservation '%s' is CANCELLED, deleting before recreating", reservation_name)
        athena.delete_capacity_reservation(Name=reservation_name)
        # Poll until deletion completes, then fall through to create.
        for attempt in range(1, _CANCELLED_DELETE_POLL_MAX_ATTEMPTS + 1):
            time.sleep(_CANCELLED_DELETE_POLL_INTERVAL_SECONDS)
            try:
                poll_response = athena.get_capacity_reservation(Name=reservation_name)
                poll_status = poll_response["CapacityReservation"]["Status"]
                logger.info(
                    "  Poll attempt %d/%d: status=%s",
                    attempt,
                    _CANCELLED_DELETE_POLL_MAX_ATTEMPTS,
                    poll_status,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "InvalidRequestException":
                    logger.info("Reservation '%s' deleted, proceeding with creation", reservation_name)
                    status = None
                    break
                raise
        else:
            raise RuntimeError(
                f"Reservation '{reservation_name}' not fully deleted after"
                f" {_CANCELLED_DELETE_POLL_MAX_ATTEMPTS} attempts."
            )

    if status is None:
        logger.info("Reservation '%s' not found, creating new reservation", reservation_name)
        athena.create_capacity_reservation(
            TargetDpus=target_dpus,
            Name=reservation_name,
        )
        try:
            athena.put_capacity_assignment_configuration(
                CapacityReservationName=reservation_name,
                CapacityAssignments=[{"WorkGroupNames": workgroup_names}],
            )
        except Exception as orig_exc:
            # Cancel is async: the reservation transitions to CANCELLING and eventually
            # CANCELLED. If the caller retries immediately, _activate_capacity_reservation_direct
            # will see CANCELLING and raise RuntimeError. The caller should wait before retrying.
            try:
                logger.warning("Failed to assign workgroups, cancelling reservation")
                athena.cancel_capacity_reservation(Name=reservation_name)
            except Exception as cancel_exc:
                logger.error(
                    "Also failed to cancel reservation: %s. "
                    "Manual cleanup of capacity reservation '%s' may be required.",
                    cancel_exc,
                    reservation_name,
                )
            raise orig_exc
    elif status == "UPDATE_PENDING":
        logger.info("Reservation '%s' is UPDATE_PENDING, skipping DPU update", reservation_name)
    elif status == "ACTIVE":
        current_dpus = int(response["CapacityReservation"]["TargetDpus"])
        if current_dpus >= target_dpus:
            logger.info(
                "Reservation DPU at %d >= floor %d, no update needed",
                current_dpus,
                target_dpus,
            )
        else:
            logger.info("Reservation DPU at %d < floor %d, updating", current_dpus, target_dpus)
            athena.update_capacity_reservation(Name=reservation_name, TargetDpus=target_dpus)
    elif status == "CANCELLING":
        raise RuntimeError(f"Reservation '{reservation_name}' is CANCELLING. Cannot activate until fully cancelled.")
    else:
        raise RuntimeError(f"Unexpected reservation status: {status}")


_DeactivateResult = Literal["cancelled", "no-op", "update_pending_timeout"]


def _deactivate_capacity_reservation_direct(reservation_name: str) -> _DeactivateResult:
    """Deactivate the Athena Capacity Reservation directly via Athena API.

    Mirrors Lambda handle_deactivate logic:
    - UPDATE_PENDING: poll up to _DEACTIVATE_DIRECT_POLL_MAX_ATTEMPTS times
    - ACTIVE: cancel_capacity_reservation
    - None/CANCELLED/CANCELLING/FAILED: no-op

    Returns:
        "cancelled"             - reservation was cancelled successfully.
        "no-op"                 - nothing to do (already cancelled/cancelling/not found).
        "update_pending_timeout" - still UPDATE_PENDING after max poll attempts; caller decides.

    Raises ClientError or RuntimeError on other failures.
    """
    athena = boto3.client("athena")

    try:
        response = athena.get_capacity_reservation(Name=reservation_name)
        status: str | None = response["CapacityReservation"]["Status"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidRequestException":
            status = None
        else:
            raise

    if status == "UPDATE_PENDING":
        logger.info(
            "Reservation is UPDATE_PENDING, polling up to %d times (%ds interval)",
            _DEACTIVATE_DIRECT_POLL_MAX_ATTEMPTS,
            _DEACTIVATE_DIRECT_POLL_INTERVAL_SECONDS,
        )
        for attempt in range(1, _DEACTIVATE_DIRECT_POLL_MAX_ATTEMPTS + 1):
            time.sleep(_DEACTIVATE_DIRECT_POLL_INTERVAL_SECONDS)
            try:
                poll_response = athena.get_capacity_reservation(Name=reservation_name)
                status = poll_response["CapacityReservation"]["Status"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "InvalidRequestException":
                    status = None
                else:
                    raise
            logger.info(
                "  Poll attempt %d/%d: status=%s",
                attempt,
                _DEACTIVATE_DIRECT_POLL_MAX_ATTEMPTS,
                status,
            )
            if status != "UPDATE_PENDING":
                break
        else:
            logger.warning("Reservation still UPDATE_PENDING after polling")
            return "update_pending_timeout"

    if status == "ACTIVE":
        logger.info("Reservation '%s' is ACTIVE, cancelling", reservation_name)
        athena.cancel_capacity_reservation(Name=reservation_name)
        return "cancelled"
    elif status is None or status in ("CANCELLED", "CANCELLING", "FAILED"):
        logger.info("Reservation status=%s, nothing to do", status)
        return "no-op"
    else:
        # Any unknown status (e.g. future AWS status codes). UPDATE_PENDING is resolved
        # above (loop exits early or returns "update_pending_timeout" before reaching here).
        raise RuntimeError(f"Unexpected reservation status: {status}")


def _poll_until_active(reservation_name: str) -> None:
    """Poll the Athena Capacity Reservation until it is ACTIVE."""
    athena_client = boto3.client("athena")
    timeout_at = time.time() + POLL_TIMEOUT_SECONDS
    logger.info(
        "Polling for ACTIVE status (timeout: %ds, interval: %ds)...",
        POLL_TIMEOUT_SECONDS,
        POLL_INTERVAL_SECONDS,
    )

    consecutive_errors = 0
    while time.time() < timeout_at:
        try:
            response = athena_client.get_capacity_reservation(Name=reservation_name)
            consecutive_errors = 0
            status = response["CapacityReservation"]["Status"]
            logger.info("  Reservation status: %s", status)
            if status == "ACTIVE":
                return
            if status in ("CANCELLED", "CANCELLING", "FAILED"):
                raise RuntimeError(f"Reservation entered terminal state: {status}")
        except ClientError as e:
            consecutive_errors += 1
            logger.warning(
                "  Error polling reservation status (%d/%d): %s",
                consecutive_errors,
                POLL_MAX_CONSECUTIVE_ERRORS,
                e,
            )
            if consecutive_errors >= POLL_MAX_CONSECUTIVE_ERRORS:
                raise TimeoutError(f"Polling aborted after {consecutive_errors} consecutive errors: {e}") from e

        remaining = timeout_at - time.time()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SECONDS, remaining))

    raise TimeoutError(
        f"Athena Capacity Reservation '{reservation_name}' did not become ACTIVE within {POLL_TIMEOUT_SECONDS} seconds."
    )
