"""Centralized settings for athena_capacity_reservation using pydantic-settings."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings
from pydantic_settings.sources.providers.env import EnvSettingsSource

from athena_capacity_reservation.monitor import _MonitorConfig

logger = logging.getLogger(__name__)


def _parse_csv(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(item) for item in v]
    if isinstance(v, str):
        return [item.strip() for item in v.split(",") if item.strip()]
    return []


class _CsvEnvSettingsSource(EnvSettingsSource):
    """Custom env source that parses CSV strings for workgroup_names
    instead of attempting JSON decode."""

    def decode_complex_value(self, field_name: str, field: Any, value: Any) -> Any:
        if field_name == "workgroup_names" and isinstance(value, str):
            return _parse_csv(value)
        return super().decode_complex_value(field_name, field, value)


class Settings(BaseSettings):
    model_config = {"env_prefix": "ATHENA_CR_"}

    reservation_name: str | None = None
    workgroup_names: list[str] = Field(default_factory=list)
    dpus: int | None = Field(default=None, gt=0)
    min_dpus: int | None = Field(default=None, gt=0)
    max_dpus: int | None = Field(default=None, gt=0)
    scale_step_dpus: int = Field(default=8, gt=0)
    scale_out_threshold: float = Field(default=80.0, gt=0, lt=100)
    scale_in_threshold: float = Field(default=30.0, gt=0, lt=100)
    monitor_interval: int = Field(default=60, gt=0)
    cooldown_seconds: int = Field(default=300, gt=0)
    queued_ticks_for_scale_out: int = Field(default=2, gt=0)
    low_ticks_for_scale_in: int = Field(default=2, gt=0)

    slack_token: str | None = None
    slack_channel: str | None = None
    slack_state_file: Path = Field(
        default=Path(tempfile.gettempdir()) / "slack_state.json",
    )
    capacity_pid_file: Path = Field(
        default=Path(tempfile.gettempdir()) / "capacity_monitor.pid",
    )

    @field_validator("workgroup_names", mode="before")
    @classmethod
    def _parse_workgroup_names(cls, v: Any) -> list[str]:
        return _parse_csv(v)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        return (
            init_settings,
            _CsvEnvSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

    @model_validator(mode="after")
    def _resolve_slack_env_fallback(self) -> Settings:
        """Fall back to non-prefixed SLACK_TOKEN / SLACK_CHANNEL env vars
        when the prefixed ATHENA_CR_SLACK_TOKEN / ATHENA_CR_SLACK_CHANNEL are not set."""
        if self.slack_token is None:
            self.slack_token = os.environ.get("SLACK_TOKEN")
        if self.slack_channel is None:
            self.slack_channel = os.environ.get("SLACK_CHANNEL")
        return self

    @model_validator(mode="after")
    def _resolve_dpu_defaults(self) -> Settings:
        if self.min_dpus is None and self.dpus is not None:
            self.min_dpus = self.dpus
            logger.info("min_dpus not set. Derived from dpus: %d", self.dpus)
        if self.max_dpus is None and self.min_dpus is not None:
            self.max_dpus = self.min_dpus
            logger.info("max_dpus not set. Defaulting to min_dpus=%d (no autoscaling).", self.min_dpus)
        if self.min_dpus is not None and self.max_dpus is not None and self.min_dpus > self.max_dpus:
            raise ValueError(f"min_dpus ({self.min_dpus}) must not exceed max_dpus ({self.max_dpus}).")
        if (
            self.dpus is not None
            and self.min_dpus is not None
            and self.max_dpus is not None
            and not (self.min_dpus <= self.dpus <= self.max_dpus)
        ):
            raise ValueError(
                f"dpus ({self.dpus}) must be between min_dpus ({self.min_dpus}) and max_dpus ({self.max_dpus})."
            )
        return self

    def build_monitor_config(self) -> _MonitorConfig:
        if not self.reservation_name:
            raise RuntimeError("reservation_name is not set.")
        if self.min_dpus is None:
            raise RuntimeError("min_dpus (or dpus) must be set for monitor.")
        if self.max_dpus is None:
            raise RuntimeError("max_dpus must be set for monitor.")

        if not self.workgroup_names:
            logger.info("workgroup_names not set or empty; scale-out query count gate disabled.")

        if self.scale_in_threshold >= self.scale_out_threshold:
            logger.warning(
                "scale_in_threshold (%f) >= scale_out_threshold (%f). This may cause oscillating scale events.",
                self.scale_in_threshold,
                self.scale_out_threshold,
            )

        return _MonitorConfig(
            reservation_name=self.reservation_name,
            min_dpus=self.min_dpus,
            max_dpus=self.max_dpus,
            state_file=self.slack_state_file,
            scale_out_threshold=self.scale_out_threshold,
            scale_in_threshold=self.scale_in_threshold,
            scale_step_dpus=self.scale_step_dpus,
            monitor_interval_seconds=self.monitor_interval,
            cooldown_seconds=self.cooldown_seconds,
            workgroup_names=self.workgroup_names,
            min_queued_ticks=self.queued_ticks_for_scale_out,
            min_low_ticks=self.low_ticks_for_scale_in,
            slack_token=self.slack_token,
            slack_channel=self.slack_channel,
        )
