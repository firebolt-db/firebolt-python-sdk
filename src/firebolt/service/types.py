from enum import Enum
from types import DynamicClassAttribute


class EngineType(Enum):
    GENERAL_PURPOSE = "GENERAL_PURPOSE"
    DATA_ANALYTICS = "DATA_ANALYTICS"

    @DynamicClassAttribute
    def api_settings_preset_name(self) -> str:
        return {
            EngineType.GENERAL_PURPOSE: "ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            EngineType.DATA_ANALYTICS: "ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
        }[self]


class WarmupMethod(Enum):
    MINIMAL = "MINIMAL"
    PRELOAD_INDEXES = "PRELOAD_INDEXES"
    PRELOAD_ALL_DATA = "PRELOAD_ALL_DATA"

    @DynamicClassAttribute
    def api_name(self) -> str:
        return {
            WarmupMethod.MINIMAL: "ENGINE_SETTINGS_WARM_UP_MINIMAL",
            WarmupMethod.PRELOAD_INDEXES: "ENGINE_SETTINGS_WARM_UP_INDEXES",
            WarmupMethod.PRELOAD_ALL_DATA: "ENGINE_SETTINGS_WARM_UP_ALL",
        }[self]


class EngineStatusSummary(Enum):
    ENGINE_STATUS_SUMMARY_UNSPECIFIED = "ENGINE_STATUS_SUMMARY_UNSPECIFIED"

    # Fully stopped.
    ENGINE_STATUS_SUMMARY_STOPPED = "ENGINE_STATUS_SUMMARY_STOPPED"

    # Provisioning process is in progress.
    # We are creating cloud infra for this engine.
    ENGINE_STATUS_SUMMARY_STARTING = "ENGINE_STATUS_SUMMARY_STARTING"

    # Provisioning process is complete.
    # We are now waiting for PackDB cluster to initialize and start.
    ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING = (
        "ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING"
    )

    # Fully started.
    # Engine is ready to serve requests.
    ENGINE_STATUS_SUMMARY_RUNNING = "ENGINE_STATUS_SUMMARY_RUNNING"

    # Version of the PackDB is changing.
    # This is zero downtime operation that does not affect engine work.s
    # This status is reserved for future use (not used fow now).
    ENGINE_STATUS_SUMMARY_UPGRADING = "ENGINE_STATUS_SUMMARY_UPGRADING"

    # Hard restart (full stop/start cycle) is in progress.
    # Underlying infrastructure is being recreated.
    ENGINE_STATUS_SUMMARY_RESTARTING = "ENGINE_STATUS_SUMMARY_RESTARTING"

    # Hard restart (full stop/start cycle) is in progress.
    # Underlying infrastructure is ready, waiting for
    # PackDB cluster to initialize and start.
    # This status is logically the same as ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING,
    # but used during restart cycle.
    ENGINE_STATUS_SUMMARY_RESTARTING_INITIALIZING = (
        "ENGINE_STATUS_SUMMARY_RESTARTING_INITIALIZING"
    )

    # Underlying infrastructure has issues and is being repaired.
    # Engine is still running, but it's not fully healthy and some queries may fail.
    ENGINE_STATUS_SUMMARY_REPAIRING = "ENGINE_STATUS_SUMMARY_REPAIRING"

    # Stop is in progress.
    ENGINE_STATUS_SUMMARY_STOPPING = "ENGINE_STATUS_SUMMARY_STOPPING"

    # Termination is in progress.
    # All infrastructure that belongs to this engine will be completely destroyed.
    ENGINE_STATUS_SUMMARY_DELETING = "ENGINE_STATUS_SUMMARY_DELETING"

    # Infrastructure is terminated, engine data is deleted.
    ENGINE_STATUS_SUMMARY_DELETED = "ENGINE_STATUS_SUMMARY_DELETED"

    # Failed to start or stop.
    # This status only indicates that there were issues during provisioning operations.
    # If engine enters this status,
    # all infrastructure should be stopped/terminated already.
    ENGINE_STATUS_SUMMARY_FAILED = "ENGINE_STATUS_SUMMARY_FAILED"
