from enum import Enum


class EngineType(Enum):
    GENERAL_PURPOSE = "GENERAL_PURPOSE"
    DATA_ANALYTICS = "DATA_ANALYTICS"

    @classmethod
    def from_display_name(cls, display_name: str) -> "EngineType":
        return {
            "General Purpose": cls.GENERAL_PURPOSE,
            "Analytics": cls.DATA_ANALYTICS,
        }[display_name]

    def __str__(self) -> str:
        return self.value


class WarmupMethod(Enum):
    MINIMAL = "MINIMAL"
    PRELOAD_INDEXES = "PRELOAD_INDEXES"
    PRELOAD_ALL_DATA = "PRELOAD_ALL_DATA"

    @classmethod
    def from_display_name(cls, display_name: str) -> "WarmupMethod":
        return {
            "Minimal": cls.MINIMAL,
            "Indexes": cls.PRELOAD_INDEXES,
            "All": cls.PRELOAD_ALL_DATA,
        }[display_name]

    def __str__(self) -> str:
        return self.value


class EngineStatus(Enum):
    """
    Detailed engine status.

    See: https://docs.firebolt.io/working-with-engines/understanding-engine-fundamentals.html
    """  # noqa

    STARTING = "Starting"
    STARTED = "Started"
    RUNNING = "Running"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    DROPPING = "Dropping"
    REPAIRING = "Repairing"
    FAILED = "Failed"

    def __str__(self) -> str:
        return self.value


class DatabaseOrder(Enum):
    DATABASE_ORDER_UNSPECIFIED = "DATABASE_ORDER_UNSPECIFIED"
    DATABASE_ORDER_NAME_ASC = "DATABASE_ORDER_NAME_ASC"
    DATABASE_ORDER_NAME_DESC = "DATABASE_ORDER_NAME_DESC"
    DATABASE_ORDER_COMPUTE_REGION_ID_ASC = "DATABASE_ORDER_COMPUTE_REGION_ID_ASC"
    DATABASE_ORDER_COMPUTE_REGION_ID_DESC = "DATABASE_ORDER_COMPUTE_REGION_ID_DESC"
    DATABASE_ORDER_DATA_SIZE_FULL_ASC = "DATABASE_ORDER_DATA_SIZE_FULL_ASC"
    DATABASE_ORDER_DATA_SIZE_FULL_DESC = "DATABASE_ORDER_DATA_SIZE_FULL_DESC"
    DATABASE_ORDER_DATA_SIZE_COMPRESSED_ASC = "DATABASE_ORDER_DATA_SIZE_COMPRESSED_ASC"
    DATABASE_ORDER_DATA_SIZE_COMPRESSED_DESC = (
        "DATABASE_ORDER_DATA_SIZE_COMPRESSED_DESC"
    )
    DATABASE_ORDER_CREATE_TIME_ASC = "DATABASE_ORDER_CREATE_TIME_ASC"
    DATABASE_ORDER_CREATE_TIME_DESC = "DATABASE_ORDER_CREATE_TIME_DESC"
    DATABASE_ORDER_CREATE_ACTOR_ASC = "DATABASE_ORDER_CREATE_ACTOR_ASC"
    DATABASE_ORDER_CREATE_ACTOR_DESC = "DATABASE_ORDER_CREATE_ACTOR_DESC"
    DATABASE_ORDER_LAST_UPDATE_TIME_ASC = "DATABASE_ORDER_LAST_UPDATE_TIME_ASC"
    DATABASE_ORDER_LAST_UPDATE_TIME_DESC = "DATABASE_ORDER_LAST_UPDATE_TIME_DESC"
    DATABASE_ORDER_LAST_UPDATE_ACTOR_ASC = "DATABASE_ORDER_LAST_UPDATE_ACTOR_ASC"
    DATABASE_ORDER_LAST_UPDATE_ACTOR_DESC = "DATABASE_ORDER_LAST_UPDATE_ACTOR_DESC"
