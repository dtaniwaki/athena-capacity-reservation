"""Constants for athena_capacity_reservation."""

# Polling settings (activate)
POLL_INTERVAL_SECONDS = 30
POLL_TIMEOUT_SECONDS = 600  # 10 minutes
POLL_MAX_CONSECUTIVE_ERRORS = 3  # abort after this many consecutive API errors

# Attachment sidebar colors
COLOR_SUCCESS = "#2eb886"  # green
COLOR_FAILURE = "#e01e5a"  # red
COLOR_SCALE = COLOR_SUCCESS
