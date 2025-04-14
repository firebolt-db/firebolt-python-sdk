from __future__ import annotations

from enum import Enum

KEEPALIVE_FLAG: int = 1
KEEPIDLE_RATE: int = 60  # seconds
DEFAULT_TIMEOUT_SECONDS: int = 60

# Running statuses in information schema
ENGINE_STATUS_RUNNING_LIST = ["RUNNING", "Running", "ENGINE_STATE_RUNNING"]
JSON_OUTPUT_FORMAT = "JSON_Compact"
JSON_LINES_OUTPUT_FORMAT = "JSONLines_Compact"


class CursorState(Enum):
    NONE = 1
    ERROR = 2
    DONE = 3
    CLOSED = 4


# Parameters that should be set using USE instead of SET
USE_PARAMETER_LIST = ["database", "engine"]
# parameters that can only be set by the backend
DISALLOWED_PARAMETER_LIST = ["output_format"]
# parameters that are set by the backend and should not be set by the user
IMMUTABLE_PARAMETER_LIST = USE_PARAMETER_LIST + DISALLOWED_PARAMETER_LIST
UPDATE_ENDPOINT_HEADER = "Firebolt-Update-Endpoint"
UPDATE_PARAMETERS_HEADER = "Firebolt-Update-Parameters"
RESET_SESSION_HEADER = "Firebolt-Reset-Session"
