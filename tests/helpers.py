"""Shared test helpers for athena_capacity_reservation tests."""

from athena_capacity_reservation.monitor import _MonitorConfig
from athena_capacity_reservation.settings import Settings


def _settings(**kwargs: object) -> Settings:
    """Build a Settings object with sensible defaults for tests."""
    defaults: dict[str, object] = {
        "reservation_name": "my-reservation",
        "workgroup_names": ["my-workgroup"],
        "dpus": 8,
    }
    defaults.update(kwargs)
    return Settings(**defaults)  # type: ignore[arg-type]


def _cfg(**kwargs: object) -> _MonitorConfig:
    """Build a _MonitorConfig with sensible defaults for tests."""
    defaults: dict[str, object] = {
        "reservation_name": "res",
        "min_dpus": 8,
        "max_dpus": 120,
    }
    defaults.update(kwargs)
    return _MonitorConfig(**defaults)  # type: ignore[arg-type]
