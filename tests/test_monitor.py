"""Tests for athena_capacity_reservation.monitor."""

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from helpers import _cfg

from athena_capacity_reservation.monitor import (
    ScaleCheckResult,
    _check_and_scale,
    _daemonize,
    _get_dpu_metrics,
    _has_queued_queries,
    _scale_capacity_reservation,
)

# ---------------------------------------------------------------------------
# _has_queued_queries
# ---------------------------------------------------------------------------


def test_has_queued_queries_returns_true_when_queued() -> None:
    mock_athena = MagicMock()
    mock_athena.list_query_executions.return_value = {"QueryExecutionIds": ["id1", "id2", "id3"]}
    mock_athena.batch_get_query_execution.return_value = {
        "QueryExecutions": [
            {"Status": {"State": "QUEUED"}},
            {"Status": {"State": "RUNNING"}},
            {"Status": {"State": "QUEUED"}},
        ]
    }
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        result = _has_queued_queries(["my-workgroup"])

    assert result is True


def test_has_queued_queries_early_return_on_first_queued() -> None:
    mock_athena = MagicMock()
    mock_athena.list_query_executions.side_effect = [
        {"QueryExecutionIds": ["id1"]},
        {"QueryExecutionIds": ["id2"]},
    ]
    mock_athena.batch_get_query_execution.side_effect = [
        {"QueryExecutions": [{"Status": {"State": "QUEUED"}}]},
        {"QueryExecutions": [{"Status": {"State": "QUEUED"}}]},
    ]
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        result = _has_queued_queries(["wg1", "wg2"])

    assert result is True
    assert mock_athena.list_query_executions.call_count == 1


def test_has_queued_queries_returns_false_when_no_queued() -> None:
    mock_athena = MagicMock()
    mock_athena.list_query_executions.return_value = {"QueryExecutionIds": ["id1"]}
    mock_athena.batch_get_query_execution.return_value = {"QueryExecutions": [{"Status": {"State": "RUNNING"}}]}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        result = _has_queued_queries(["my-workgroup"])

    assert result is False


def test_has_queued_queries_returns_false_when_no_executions() -> None:
    mock_athena = MagicMock()
    mock_athena.list_query_executions.return_value = {"QueryExecutionIds": []}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        result = _has_queued_queries(["my-workgroup"])

    assert result is False
    mock_athena.batch_get_query_execution.assert_not_called()


def test_has_queued_queries_returns_none_on_api_error(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    from botocore.exceptions import ClientError

    mock_athena = MagicMock()
    mock_athena.list_query_executions.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, "ListQueryExecutions"
    )
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        result = _has_queued_queries(["my-workgroup"])

    assert result is None
    captured = capsys.readouterr()
    assert "Athena API error" in captured.err


# ---------------------------------------------------------------------------
# _get_dpu_metrics
# ---------------------------------------------------------------------------


def _make_cw_response(utilization_values: list, allocated_values: list) -> dict:  # type: ignore[type-arg]
    return {
        "MetricDataResults": [
            {"Id": "utilization", "Values": utilization_values},
            {"Id": "allocated", "Values": allocated_values},
        ]
    }


def test_get_dpu_metrics_returns_utilization_and_allocated() -> None:
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = _make_cw_response([75.0], [8.0])

        result = _get_dpu_metrics("my-reservation")

    assert result == (75.0, 8.0)


def test_get_dpu_metrics_returns_none_when_no_data() -> None:
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = _make_cw_response([], [])

        result = _get_dpu_metrics("my-reservation")

    assert result is None


def test_get_dpu_metrics_uses_most_recent_value() -> None:
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = _make_cw_response([90.0, 50.0], [8.0, 4.0])

        result = _get_dpu_metrics("my-reservation")

    assert result == (90.0, 8.0)


def test_get_dpu_metrics_returns_none_on_cloudwatch_error(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    from botocore.exceptions import ClientError

    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, "GetMetricData"
        )

        result = _get_dpu_metrics("my-reservation")

    assert result is None
    captured = capsys.readouterr()
    assert "CloudWatch API error" in captured.err


def test_get_dpu_metrics_returns_none_when_results_empty(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {"MetricDataResults": []}

        result = _get_dpu_metrics("my-reservation")

    assert result is None


# ---------------------------------------------------------------------------
# _scale_capacity_reservation
# ---------------------------------------------------------------------------


def test_scale_capacity_reservation_scales_up() -> None:
    mock_athena = MagicMock()
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 8}}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        action, from_dpus, to_dpus = _scale_capacity_reservation("res", 8, 4, 120)

    assert action == "scaled"
    assert from_dpus == 8
    assert to_dpus == 16
    mock_athena.update_capacity_reservation.assert_called_once_with(Name="res", TargetDpus=16)


def test_scale_capacity_reservation_scales_down() -> None:
    mock_athena = MagicMock()
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 24}}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        action, from_dpus, to_dpus = _scale_capacity_reservation("res", -8, 4, 120)

    assert action == "scaled"
    assert from_dpus == 24
    assert to_dpus == 16
    mock_athena.update_capacity_reservation.assert_called_once_with(Name="res", TargetDpus=16)


def test_scale_capacity_reservation_skips_when_clamped_at_max() -> None:
    mock_athena = MagicMock()
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 120}}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        action, from_dpus, to_dpus = _scale_capacity_reservation("res", 8, 4, 120)

    assert action == "skipped"
    assert from_dpus == 120
    assert to_dpus == 120
    mock_athena.update_capacity_reservation.assert_not_called()


def test_scale_capacity_reservation_skips_when_clamped_at_min() -> None:
    mock_athena = MagicMock()
    mock_athena.get_capacity_reservation.return_value = {"CapacityReservation": {"Status": "ACTIVE", "TargetDpus": 4}}
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        action, from_dpus, to_dpus = _scale_capacity_reservation("res", -8, 4, 120)

    assert action == "skipped"
    mock_athena.update_capacity_reservation.assert_not_called()


def test_scale_capacity_reservation_skips_when_update_pending(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    mock_athena = MagicMock()
    mock_athena.get_capacity_reservation.return_value = {
        "CapacityReservation": {"Status": "UPDATE_PENDING", "TargetDpus": 16}
    }
    with patch("athena_capacity_reservation.monitor.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_athena
        action, from_dpus, to_dpus = _scale_capacity_reservation("res", 8, 4, 120)

    assert action == "skipped"
    assert from_dpus == 16
    assert to_dpus == 16
    mock_athena.update_capacity_reservation.assert_not_called()
    assert "UPDATE_PENDING" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# _check_and_scale
# ---------------------------------------------------------------------------


def test_check_and_scale_no_data_returns_unchanged() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=None),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(_cfg(min_dpus=4), last)

    assert result == (last, 0, 0, 0)
    mock_scale.assert_not_called()


def test_check_and_scale_in_cooldown_preserves_ticks() -> None:
    last = time.time()
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 4.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation"),
    ):
        result = _check_and_scale(
            _cfg(min_dpus=4),
            last,
            queued_ticks=3,
            low_ticks=2,
            high_ticks=4,
        )

    assert result == (last, 3, 2, 4)


def test_check_and_scale_below_thresholds_returns_unchanged() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(50.0, 4.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(_cfg(min_dpus=4), last)

    assert result == (last, 0, 0, 0)
    mock_scale.assert_not_called()


# --- Scale-out gate tests ---


def test_check_and_scale_scale_out_deferred_when_no_queued_queries(
    capsys: pytest.CaptureFixture,  # type: ignore[type-arg]
) -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(100.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=False),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(min_dpus=4, workgroup_names=["wg"]),
            last,
        )

    assert result == (last, 0, 0, 1)
    mock_scale.assert_not_called()
    assert "Scale-out deferred" in capsys.readouterr().err


def test_check_and_scale_scale_out_deferred_on_first_queued_tick(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(100.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=True),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(min_dpus=4, workgroup_names=["wg"], min_queued_ticks=2),
            last,
            queued_ticks=0,
        )

    assert result == (last, 1, 0, 1)
    mock_scale.assert_not_called()
    assert "Scale-out deferred" in capsys.readouterr().err


def test_check_and_scale_scale_out_allowed_when_queries_are_queued() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(100.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=True),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 4, 12)),
        patch("athena_capacity_reservation.monitor.post_slack_message"),
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(
            _cfg(min_dpus=4, workgroup_names=["wg"], min_queued_ticks=2),
            last,
            queued_ticks=1,
        )

    assert result == (1001.0, 0, 0, 0)


def test_check_and_scale_scale_out_no_gate_without_workgroup_names() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries") as mock_has_queued,
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 4, 12)),
        patch("athena_capacity_reservation.monitor.post_slack_message"),
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(_cfg(min_dpus=4, min_high_ticks=1), last)

    mock_has_queued.assert_not_called()
    assert result == (1001.0, 0, 0, 0)


def test_check_and_scale_scale_out_suppressed_when_queued_count_api_error(
    capsys: pytest.CaptureFixture,  # type: ignore[type-arg]
) -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=None),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(min_dpus=4, workgroup_names=["wg"]),
            last,
        )

    assert result == (last, 0, 0, 1)
    mock_scale.assert_not_called()
    assert "Could not determine queued query status" in capsys.readouterr().err


# --- Scale-out / scale-in trigger tests ---


def test_check_and_scale_scale_out_calls_athena_directly() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(85.0, 24.0)),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 24, 36)
        ) as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message") as mock_slack,
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        _check_and_scale(_cfg(min_high_ticks=1), last)

    mock_scale.assert_called_once_with("res", 8, 8, 120, athena_client=None)
    mock_slack.assert_called_once()
    msg_text = mock_slack.call_args[0][0]
    assert "\U0001f4c8" in msg_text
    assert "scale-out" in msg_text.lower()
    assert "24\u219236" in msg_text


def test_check_and_scale_scale_in_deferred_on_first_low_tick(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(20.0, 36.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(min_low_ticks=2),
            last,
            low_ticks=0,
        )

    assert result == (last, 0, 1, 0)
    mock_scale.assert_not_called()
    assert "Scale-in deferred" in capsys.readouterr().err


def test_check_and_scale_scale_in_suppressed_when_queued_queries(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(20.0, 36.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=True),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(workgroup_names=["wg"], min_low_ticks=2),
            last,
            low_ticks=1,
        )

    assert result == (last, 0, 2, 0)
    mock_scale.assert_not_called()
    assert "Scale-in suppressed" in capsys.readouterr().err


def test_check_and_scale_scale_in_suppressed_when_queued_count_api_error(
    capsys: pytest.CaptureFixture,  # type: ignore[type-arg]
) -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(20.0, 36.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=None),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(workgroup_names=["wg"], min_low_ticks=2),
            last,
            low_ticks=1,
        )

    assert result == (last, 0, 2, 0)
    mock_scale.assert_not_called()
    assert "Could not determine queued query status" in capsys.readouterr().err


def test_check_and_scale_scale_in_calls_athena_directly() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(20.0, 36.0)),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 36, 24)
        ) as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message") as mock_slack,
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(
            _cfg(min_low_ticks=2),
            last,
            low_ticks=1,
        )

    mock_scale.assert_called_once_with("res", -8, 8, 120, athena_client=None)
    mock_slack.assert_called_once()
    msg_text = mock_slack.call_args[0][0]
    assert "\U0001f4c9" in msg_text
    assert "scale-in" in msg_text.lower()
    assert "36\u219224" in msg_text
    assert result == (1001.0, 0, 0, 0)


def test_check_and_scale_high_utilization_resets_low_ticks() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(100.0, 4.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=False),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation"),
    ):
        result = _check_and_scale(
            _cfg(min_dpus=4, workgroup_names=["wg"]),
            last,
            low_ticks=3,
        )

    assert result == (last, 0, 0, 1)


def test_check_and_scale_low_utilization_resets_queued_and_high_ticks() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(20.0, 36.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation"),
    ):
        result = _check_and_scale(
            _cfg(min_low_ticks=2),
            last,
            queued_ticks=3,
            low_ticks=0,
            high_ticks=4,
        )

    assert result == (last, 0, 1, 0)


def test_check_and_scale_scale_skipped_is_noop(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(85.0, 24.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("skipped", 120, 120)),
        patch("athena_capacity_reservation.monitor.post_slack_message") as mock_slack,
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1000.0
        result = _check_and_scale(_cfg(min_high_ticks=1), last)

    assert result == (last, 0, 0, 0)
    mock_slack.assert_not_called()
    assert "no-op" in capsys.readouterr().err


def test_check_and_scale_scale_api_error_propagates() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(85.0, 24.0)),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation",
            side_effect=ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, "UpdateCapacityReservation"
            ),
        ),
    ):
        with pytest.raises(ClientError):
            _check_and_scale(_cfg(min_high_ticks=1), last)


def test_check_and_scale_skips_when_metrics_none() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=None),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message") as mock_slack,
    ):
        result = _check_and_scale(_cfg(min_dpus=4), last)

    assert result == (last, 0, 0, 0)
    mock_scale.assert_not_called()
    mock_slack.assert_not_called()


def test_check_and_scale_skips_when_allocated_dpus_zero() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(0.0, 0.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(_cfg(min_dpus=4), last)

    assert result == (last, 0, 0, 0)
    mock_scale.assert_not_called()


# --- Sustained high utilization (high_ticks) tests ---


def test_check_and_scale_sustained_high_utilization_triggers_scale_out() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 24.0)),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 24, 32)
        ) as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message"),
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(
            _cfg(min_high_ticks=5),
            last,
            high_ticks=4,
        )

    assert result == (1001.0, 0, 0, 0)
    mock_scale.assert_called_once()


def test_check_and_scale_sustained_high_utilization_with_workgroups_no_queued() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 24.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=False),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 24, 32)
        ) as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message"),
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(
            _cfg(workgroup_names=["wg"], min_high_ticks=5),
            last,
            high_ticks=4,
        )

    assert result == (1001.0, 0, 0, 0)
    mock_scale.assert_called_once()


def test_check_and_scale_high_ticks_deferred_without_workgroups(capsys: pytest.CaptureFixture) -> None:  # type: ignore[type-arg]
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 24.0)),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(min_high_ticks=5),
            last,
            high_ticks=2,
        )

    assert result == (last, 0, 0, 3)
    mock_scale.assert_not_called()
    assert "Scale-out deferred" in capsys.readouterr().err


def test_check_and_scale_sustained_path_with_workgroups_and_queued() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 24.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=True),
        patch(
            "athena_capacity_reservation.monitor._scale_capacity_reservation", return_value=("scaled", 24, 32)
        ) as mock_scale,
        patch("athena_capacity_reservation.monitor.post_slack_message"),
        patch("athena_capacity_reservation.monitor.time") as mock_time,
    ):
        mock_time.time.return_value = 1001.0
        result = _check_and_scale(
            _cfg(workgroup_names=["wg"], min_queued_ticks=3, min_high_ticks=5),
            last,
            queued_ticks=0,
            high_ticks=4,
        )

    assert result == (1001.0, 0, 0, 0)
    mock_scale.assert_called_once()


def test_check_and_scale_high_ticks_preserved_on_queued_api_error() -> None:
    last = 0.0
    with (
        patch("athena_capacity_reservation.monitor._get_dpu_metrics", return_value=(90.0, 24.0)),
        patch("athena_capacity_reservation.monitor._has_queued_queries", return_value=None),
        patch("athena_capacity_reservation.monitor._scale_capacity_reservation") as mock_scale,
    ):
        result = _check_and_scale(
            _cfg(workgroup_names=["wg"], min_high_ticks=5),
            last,
            high_ticks=3,
        )

    assert result == (last, 0, 0, 4)
    mock_scale.assert_not_called()


# ---------------------------------------------------------------------------
# _run_monitor_loop
# ---------------------------------------------------------------------------


def test_monitor_loop_does_not_reset_last_scale_time_on_error() -> None:
    from athena_capacity_reservation.monitor import _run_monitor_loop

    cfg = _cfg(monitor_interval_seconds=0)

    stop_event = threading.Event()
    received_last_scale_times: list[float] = []
    call_count = 0

    def fake_check(_cfg: object, last_scale_time: float, *args: object, **kwargs: object) -> ScaleCheckResult:
        nonlocal call_count
        call_count += 1
        received_last_scale_times.append(last_scale_time)
        if call_count == 1:
            raise RuntimeError("API error")
        stop_event.set()
        return ScaleCheckResult(last_scale_time, 0, 0, 0)

    with (
        patch("athena_capacity_reservation.monitor.boto3"),
        patch("athena_capacity_reservation.monitor._check_and_scale", side_effect=fake_check),
    ):
        _run_monitor_loop(cfg, stop_event=stop_event)

    assert call_count == 2
    assert received_last_scale_times[0] == received_last_scale_times[1]


# ---------------------------------------------------------------------------
# _daemonize
# ---------------------------------------------------------------------------


def test_daemonize_parent_writes_pid_and_exits(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    with (
        patch("os.fork", return_value=42),
        patch("os.setsid"),
        pytest.raises(SystemExit) as exc_info,
    ):
        _daemonize(pid_file)
    assert exc_info.value.code == 0
    assert pid_file.read_text() == "42"


def test_daemonize_child_calls_setsid(tmp_path: Path) -> None:
    pid_file = tmp_path / "monitor.pid"
    with (
        patch("os.fork", return_value=0),
        patch("os.setsid") as mock_setsid,
        patch("os.open", return_value=99),
        patch("os.dup2") as mock_dup2,
        patch("os.close") as mock_close,
    ):
        _daemonize(pid_file)
    mock_setsid.assert_called_once()
    assert mock_dup2.call_count == 3
    assert mock_close.call_count == 2
    assert not pid_file.exists()
