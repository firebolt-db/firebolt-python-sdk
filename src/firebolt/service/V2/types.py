from enum import Enum
from typing import Any


class EngineStatus(Enum):
    """
    Detailed engine status.
    """

    STARTING = "STARTING"
    STARTED = "STARTED"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    DROPPING = "DROPPING"
    REPAIRING = "REPAIRING"
    FAILED = "FAILED"
    DELETING = "DELETING"
    RESIZING = "RESIZING"
    DRAINING = "DRAINING"
    UNKNOWN = "UNKNOWN"  # status could not be determined

    @classmethod
    def _missing_(cls, value: Any) -> "EngineStatus":
        return cls.UNKNOWN

    def __str__(self) -> str:
        return self.value
