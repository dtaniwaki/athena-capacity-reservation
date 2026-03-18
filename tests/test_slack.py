"""Tests for athena_capacity_reservation.slack."""

from pathlib import Path

from athena_capacity_reservation.slack import load_state


def test_load_state_missing_file(tmp_path: Path) -> None:
    assert load_state(tmp_path / "nonexistent.json") == {}


def test_load_state_valid(tmp_path: Path) -> None:
    f = tmp_path / "state.json"
    f.write_text('{"thread_ts": "123.456"}')
    assert load_state(f) == {"thread_ts": "123.456"}


def test_load_state_invalid_json(tmp_path: Path) -> None:
    f = tmp_path / "state.json"
    f.write_text("not json")
    assert load_state(f) == {}
