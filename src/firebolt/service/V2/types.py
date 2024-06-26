from enum import Enum


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

    def __str__(self) -> str:
        return self.value
