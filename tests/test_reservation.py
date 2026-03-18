"""Tests for athena_capacity_reservation.reservation."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from athena_capacity_reservation.constants import POLL_MAX_CONSECUTIVE_ERRORS
from athena_capacity_reservation.reservation import (
    _activate_capacity_reservation_direct,
    _deactivate_capacity_reservation_direct,
    _poll_until_active,
)

# ---------------------------------------------------------------------------
# _poll_until_active
# ---------------------------------------------------------------------------


@patch("athena_capacity_reservation.reservation.boto3.client")
@patch("athena_capacity_reservation.reservation.time.sleep")
def test_poll_until_active_fails_fast_on_consecutive_errors(
    mock_sleep: MagicMock,
    mock_boto_client: MagicMock,
) -> None:
    import botocore.exceptions

    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "Permission denied"}}, "GetCapacityReservation"
    )

    with pytest.raises(TimeoutError):
        _poll_until_active("my-reservation")

    assert mock_athena.get_capacity_reservation.call_count == POLL_MAX_CONSECUTIVE_ERRORS


@patch("athena_capacity_reservation.reservation.boto3.client")
@patch("athena_capacity_reservation.reservation.time.sleep")
def test_poll_until_active_resets_error_count_on_success(
    mock_sleep: MagicMock,
    mock_boto_client: MagicMock,
) -> None:
    import botocore.exceptions

    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena

    client_error = botocore.exceptions.ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "error"}}, "GetCapacityReservation"
    )
    mock_athena.get_capacity_reservation.side_effect = [
        client_error,
        {"CapacityReservation": {"Status": "PENDING"}},
        client_error,
        client_error,
        client_error,
    ]

    with pytest.raises(TimeoutError):
        _poll_until_active("my-reservation")

    assert mock_athena.get_capacity_reservation.call_count == 5


@patch("athena_capacity_reservation.reservation.boto3.client")
@patch("athena_capacity_reservation.reservation.time.sleep")
@patch("athena_capacity_reservation.reservation.time.time")
def test_poll_until_active_sleep_capped_at_remaining_time(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    mock_boto_client: MagicMock,
) -> None:
    from athena_capacity_reservation.constants import POLL_INTERVAL_SECONDS, POLL_TIMEOUT_SECONDS

    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "PENDING"}}

    start = 1000.0
    timeout_at = start + POLL_TIMEOUT_SECONDS
    mock_time.side_effect = [start, 0.0, timeout_at - 5, 0.0, timeout_at - 5, timeout_at + 1]

    with pytest.raises(TimeoutError):
        _poll_until_active("my-reservation")

    sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
    assert all(s <= POLL_INTERVAL_SECONDS for s in sleep_calls)
    assert any(s < POLL_INTERVAL_SECONDS for s in sleep_calls)


# ---------------------------------------------------------------------------
# _activate_capacity_reservation_direct
# ---------------------------------------------------------------------------


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_creates_when_not_found(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.side_effect = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "not found"}}, "GetCapacityReservation"
    )

    _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.create_capacity_reservation.assert_called_once_with(TargetDpus=8, Name="my-res")
    mock_athena.put_capacity_assignment_configuration.assert_called_once_with(
        CapacityReservationName="my-res",
        CapacityAssignments=[{"WorkGroupNames": ["wg1"]}],
    )


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_updates_when_active_and_underprovision(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 4}}

    _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.update_capacity_reservation.assert_called_once_with(Name="my-res", TargetDpus=8)
    assert mock_athena.get_capacity_reservation.call_count == 1


@patch("athena_capacity_reservation.reservation.time.sleep")
@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_creates_after_polling_when_cancelled(
    mock_boto_client: MagicMock, mock_sleep: MagicMock
) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena

    not_found_error = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "not found"}}, "GetCapacityReservation"
    )
    mock_athena.get_capacity_reservation.side_effect = [
        {"CapacityReservation": {"Status": "CANCELLED"}},
        not_found_error,
    ]

    _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.delete_capacity_reservation.assert_called_once_with(Name="my-res")
    mock_athena.create_capacity_reservation.assert_called_once()
    mock_athena.put_capacity_assignment_configuration.assert_called_once()


@patch("athena_capacity_reservation.reservation.time.sleep")
@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_raises_when_cancelled_delete_timeout(
    mock_boto_client: MagicMock, mock_sleep: MagicMock
) -> None:
    from athena_capacity_reservation.reservation import _CANCELLED_DELETE_POLL_MAX_ATTEMPTS

    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "CANCELLED"}}

    with pytest.raises(RuntimeError, match="not fully deleted"):
        _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.delete_capacity_reservation.assert_called_once_with(Name="my-res")
    assert mock_athena.get_capacity_reservation.call_count == 1 + _CANCELLED_DELETE_POLL_MAX_ATTEMPTS
    mock_athena.create_capacity_reservation.assert_not_called()


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_noop_when_active_and_overprovision(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 16}}

    _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.update_capacity_reservation.assert_not_called()


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_noop_when_update_pending(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {
        "CapacityReservation": {"Status": "UPDATE_PENDING", "TargetDpus": 8}
    }

    _activate_capacity_reservation_direct("my-res", ["wg1"], 8)

    mock_athena.update_capacity_reservation.assert_not_called()
    mock_athena.create_capacity_reservation.assert_not_called()


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_activate_direct_raises_when_cancelling(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {
        "CapacityReservation": {"Status": "CANCELLING", "TargetDpus": 8}
    }

    with pytest.raises(RuntimeError, match="CANCELLING"):
        _activate_capacity_reservation_direct("my-res", ["wg1"], 8)


# ---------------------------------------------------------------------------
# _deactivate_capacity_reservation_direct
# ---------------------------------------------------------------------------


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_deactivate_direct_cancels_when_active(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE"}}

    result = _deactivate_capacity_reservation_direct("my-res")

    mock_athena.cancel_capacity_reservation.assert_called_once_with(Name="my-res")
    assert result == "cancelled"


@patch("athena_capacity_reservation.reservation.boto3.client")
def test_deactivate_direct_noop_when_already_cancelled(mock_boto_client: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "CANCELLED"}}

    result = _deactivate_capacity_reservation_direct("my-res")

    mock_athena.cancel_capacity_reservation.assert_not_called()
    assert result == "no-op"


@patch("athena_capacity_reservation.reservation.time.sleep")
@patch("athena_capacity_reservation.reservation.boto3.client")
def test_deactivate_direct_returns_update_pending_timeout(mock_boto_client: MagicMock, mock_sleep: MagicMock) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "UPDATE_PENDING"}}

    result = _deactivate_capacity_reservation_direct("my-res")

    assert result == "update_pending_timeout"
    mock_athena.cancel_capacity_reservation.assert_not_called()


@patch("athena_capacity_reservation.reservation.time.sleep")
@patch("athena_capacity_reservation.reservation.boto3.client")
def test_deactivate_direct_cancels_after_update_pending_resolves(
    mock_boto_client: MagicMock, mock_sleep: MagicMock
) -> None:
    mock_athena = MagicMock()
    mock_boto_client.return_value = mock_athena
    mock_athena.get_capacity_reservation.side_effect = [
        {"CapacityReservation": {"Status": "UPDATE_PENDING"}},
        {"CapacityReservation": {"Status": "ACTIVE"}},
    ]

    result = _deactivate_capacity_reservation_direct("my-res")

    mock_athena.cancel_capacity_reservation.assert_called_once_with(Name="my-res")
    assert result == "cancelled"
