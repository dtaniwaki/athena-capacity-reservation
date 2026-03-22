"""Tests for athena_capacity_reservation.cli."""

from unittest.mock import patch

from click.testing import CliRunner

from athena_capacity_reservation.cli import main


def test_monitor_start_accepts_high_ticks_for_scale_out() -> None:
    runner = CliRunner()
    with patch("athena_capacity_reservation.cli.cmd_monitor_start") as mock_start:
        result = runner.invoke(
            main,
            [
                "monitor",
                "start",
                "--reservation-name",
                "res",
                "--min-dpus",
                "8",
                "--max-dpus",
                "120",
                "--high-ticks-for-scale-out",
                "7",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_start.assert_called_once()
    settings = mock_start.call_args[0][0]
    assert settings.high_ticks_for_scale_out == 7


def test_monitor_start_high_ticks_default() -> None:
    runner = CliRunner()
    with patch("athena_capacity_reservation.cli.cmd_monitor_start") as mock_start:
        result = runner.invoke(
            main,
            [
                "monitor",
                "start",
                "--reservation-name",
                "res",
                "--min-dpus",
                "8",
                "--max-dpus",
                "120",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_start.assert_called_once()
    settings = mock_start.call_args[0][0]
    assert settings.high_ticks_for_scale_out == 5
