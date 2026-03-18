"""Tests for Settings (pydantic-settings based configuration)."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from athena_capacity_reservation.settings import Settings

# ---------------------------------------------------------------------------
# env_prefix and basic field resolution
# ---------------------------------------------------------------------------


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_RESERVATION_NAME", "my-res")
    monkeypatch.setenv("ATHENA_CR_WORKGROUP_NAMES", "wg1,wg2")
    monkeypatch.setenv("ATHENA_CR_DPUS", "8")
    s = Settings()
    assert s.reservation_name == "my-res"
    assert s.workgroup_names == ["wg1", "wg2"]
    assert s.dpus == 8


def test_settings_cli_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_DPUS", "4")
    s = Settings(dpus=16)
    assert s.dpus == 16


# ---------------------------------------------------------------------------
# CSV parsing for workgroup_names
# ---------------------------------------------------------------------------


def test_settings_workgroup_names_csv_string() -> None:
    s = Settings(workgroup_names="wg1, wg2 , wg3")
    assert s.workgroup_names == ["wg1", "wg2", "wg3"]


def test_settings_workgroup_names_list() -> None:
    s = Settings(workgroup_names=["a", "b"])
    assert s.workgroup_names == ["a", "b"]


def test_settings_workgroup_names_empty_string() -> None:
    s = Settings(workgroup_names="")
    assert s.workgroup_names == []


def test_settings_workgroup_names_default() -> None:
    s = Settings()
    assert s.workgroup_names == []


# ---------------------------------------------------------------------------
# DPU cross-field validation (model_validator)
# ---------------------------------------------------------------------------


def test_settings_min_derived_from_dpus() -> None:
    s = Settings(dpus=8)
    assert s.min_dpus == 8
    assert s.max_dpus == 8


def test_settings_min_derived_from_dpus_max_explicit() -> None:
    s = Settings(dpus=8, max_dpus=32)
    assert s.min_dpus == 8
    assert s.max_dpus == 32


def test_settings_max_defaults_to_min() -> None:
    s = Settings(min_dpus=16)
    assert s.max_dpus == 16


def test_settings_min_gt_max_raises() -> None:
    with pytest.raises(ValidationError, match="must not exceed"):
        Settings(min_dpus=32, max_dpus=8)


def test_settings_dpus_out_of_range_raises() -> None:
    with pytest.raises(ValidationError, match="must be between"):
        Settings(dpus=4, min_dpus=8, max_dpus=32)


def test_settings_dpus_in_range() -> None:
    s = Settings(dpus=16, min_dpus=8, max_dpus=32)
    assert s.dpus == 16
    assert s.min_dpus == 8
    assert s.max_dpus == 32


def test_settings_min_equals_max() -> None:
    s = Settings(min_dpus=8, max_dpus=8)
    assert s.min_dpus == 8
    assert s.max_dpus == 8


# ---------------------------------------------------------------------------
# Field validation (gt, lt constraints)
# ---------------------------------------------------------------------------


def test_settings_dpus_zero_raises() -> None:
    with pytest.raises(ValidationError, match="greater than 0"):
        Settings(dpus=0)


def test_settings_negative_dpus_raises() -> None:
    with pytest.raises(ValidationError, match="greater than 0"):
        Settings(dpus=-1)


def test_settings_scale_out_threshold_out_of_range() -> None:
    with pytest.raises(ValidationError):
        Settings(scale_out_threshold=0)


def test_settings_scale_out_threshold_100_raises() -> None:
    with pytest.raises(ValidationError):
        Settings(scale_out_threshold=100)


# ---------------------------------------------------------------------------
# Slack env var resolution (prefixed takes priority, non-prefixed as fallback)
# ---------------------------------------------------------------------------


def test_settings_slack_token_from_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_SLACK_TOKEN", "xoxb-prefixed")
    s = Settings()
    assert s.slack_token == "xoxb-prefixed"


def test_settings_slack_token_fallback_to_non_prefixed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-fallback")
    s = Settings()
    assert s.slack_token == "xoxb-fallback"


def test_settings_slack_token_prefixed_takes_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_SLACK_TOKEN", "xoxb-prefixed")
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-fallback")
    s = Settings()
    assert s.slack_token == "xoxb-prefixed"


def test_settings_slack_channel_from_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_SLACK_CHANNEL", "C-prefixed")
    s = Settings()
    assert s.slack_channel == "C-prefixed"


def test_settings_slack_channel_fallback_to_non_prefixed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_CHANNEL", "C-fallback")
    s = Settings()
    assert s.slack_channel == "C-fallback"


def test_settings_slack_channel_prefixed_takes_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_SLACK_CHANNEL", "C-prefixed")
    monkeypatch.setenv("SLACK_CHANNEL", "C-fallback")
    s = Settings()
    assert s.slack_channel == "C-prefixed"


def test_settings_slack_state_file_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_SLACK_STATE_FILE", "/custom/state.json")
    s = Settings()
    assert s.slack_state_file == Path("/custom/state.json")


def test_settings_capacity_pid_file_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_CR_CAPACITY_PID_FILE", "/custom/monitor.pid")
    s = Settings()
    assert s.capacity_pid_file == Path("/custom/monitor.pid")


# ---------------------------------------------------------------------------
# build_monitor_config
# ---------------------------------------------------------------------------


def test_build_monitor_config_basic() -> None:
    s = Settings(reservation_name="res", dpus=8, workgroup_names=["wg1"])
    cfg = s.build_monitor_config()
    assert cfg.reservation_name == "res"
    assert cfg.min_dpus == 8
    assert cfg.max_dpus == 8
    assert cfg.workgroup_names == ["wg1"]


def test_build_monitor_config_custom_values() -> None:
    s = Settings(
        reservation_name="res",
        min_dpus=4,
        max_dpus=32,
        scale_step_dpus=16,
        scale_out_threshold=90.0,
        scale_in_threshold=10.0,
        monitor_interval=120,
        cooldown_seconds=600,
        queued_ticks_for_scale_out=3,
        low_ticks_for_scale_in=5,
        slack_token="xoxb-test",
        slack_channel="C123",
    )
    cfg = s.build_monitor_config()
    assert cfg.scale_step_dpus == 16
    assert cfg.scale_out_threshold == 90.0
    assert cfg.scale_in_threshold == 10.0
    assert cfg.monitor_interval_seconds == 120
    assert cfg.cooldown_seconds == 600
    assert cfg.min_queued_ticks == 3
    assert cfg.min_low_ticks == 5
    assert cfg.slack_token == "xoxb-test"
    assert cfg.slack_channel == "C123"


def test_build_monitor_config_raises_without_reservation_name() -> None:
    s = Settings(dpus=8)
    with pytest.raises(RuntimeError, match="reservation_name"):
        s.build_monitor_config()


def test_build_monitor_config_raises_without_dpus() -> None:
    s = Settings(reservation_name="res")
    with pytest.raises(RuntimeError, match="min_dpus"):
        s.build_monitor_config()
