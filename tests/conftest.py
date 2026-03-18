"""Test configuration for athena_capacity_reservation tests."""

import logging
import sys

import pytest


class _DynamicStderrHandler(logging.StreamHandler):
    """A StreamHandler that always writes to the current sys.stderr.

    This is needed because pytest's capsys replaces sys.stderr with a StringIO,
    and a regular StreamHandler would hold a reference to the original sys.stderr.
    By looking up sys.stderr dynamically, we ensure capsys can capture log output.
    """

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value):
        # Ignore assignments (parent __init__ does self.stream = stream)
        pass


@pytest.fixture(autouse=True)
def configure_logging_to_stderr():
    """Configure logging to write to the current sys.stderr so capsys can capture it."""
    root_logger = logging.getLogger()
    original_level = root_logger.level
    original_handlers = root_logger.handlers[:]

    # Remove existing handlers and add dynamic stderr handler
    root_logger.handlers.clear()
    handler = _DynamicStderrHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)

    yield

    # Restore original handlers
    root_logger.handlers.clear()
    for h in original_handlers:
        root_logger.addHandler(h)
    root_logger.setLevel(original_level)
