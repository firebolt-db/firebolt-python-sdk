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


class EngineStatus(Enum):
    """
    Detailed engine status.

    See: https://api.dev.firebolt.io/devDocs#operation/coreV1GetEngine
    """

    ENGINE_STATUS_UNSPECIFIED = "ENGINE_STATUS_UNSPECIFIED"
    """ Logical record is created, however, underlying infrastructure
    is not initialized.
    In other words, this means that engine is stopped."""

    ENGINE_STATUS_CREATED = "ENGINE_STATUS_CREATED"
    """Engine status was created."""

    ENGINE_STATUS_PROVISIONING_PENDING = "ENGINE_STATUS_PROVISIONING_PENDING"
    """ Engine initialization request was sent."""

    ENGINE_STATUS_PROVISIONING_STARTED = "ENGINE_STATUS_PROVISIONING_STARTED"
    """ Engine initialization request was received
    and initialization process started."""

    ENGINE_STATUS_PROVISIONING_FINISHED = "ENGINE_STATUS_PROVISIONING_FINISHED"
    """ Engine initialization was finished successfully."""

    ENGINE_STATUS_PROVISIONING_FAILED = "ENGINE_STATUS_PROVISIONING_FAILED"
    """ Engine initialization failed due to error."""

    ENGINE_STATUS_RUNNING_IDLE = "ENGINE_STATUS_RUNNING_IDLE"
    """ Engine is initialized,
    but there are no running or starting engine revisions."""

    ENGINE_STATUS_RUNNING_REVISION_STARTING = "ENGINE_STATUS_RUNNING_REVISION_STARTING"
    """ Engine is initialized,
    there are no running engine revisions, but it's starting."""

    ENGINE_STATUS_RUNNING_REVISION_STARTUP_FAILED = (
        "ENGINE_STATUS_RUNNING_REVISION_STARTUP_FAILED"
    )
    """ Engine is initialized;
    initial revision failed to provision or start."""

    ENGINE_STATUS_RUNNING_REVISION_SERVING = "ENGINE_STATUS_RUNNING_REVISION_SERVING"
    """ Engine is ready (serves an engine revision). """

    ENGINE_STATUS_RUNNING_REVISION_CHANGING = "ENGINE_STATUS_RUNNING_REVISION_CHANGING"
    """ Engine is ready (serves an engine revision);
     zero-downtime replacement revision is starting."""

    ENGINE_STATUS_RUNNING_REVISION_CHANGE_FAILED = (
        "ENGINE_STATUS_RUNNING_REVISION_CHANGE_FAILED"
    )
    """ Engine is ready (serves an engine revision);
     replacement revision failed to provision or start."""

    ENGINE_STATUS_RUNNING_REVISION_RESTARTING = (
        "ENGINE_STATUS_RUNNING_REVISION_RESTARTING"
    )
    """ Engine is initialized;
    replacement of the revision with a downtime is in progress."""

    ENGINE_STATUS_RUNNING_REVISION_RESTART_FAILED = (
        "ENGINE_STATUS_RUNNING_REVISION_RESTART_FAILED"
    )
    """ Engine is initialized;
    replacement revision failed to provision or start."""

    ENGINE_STATUS_RUNNING_REVISIONS_TERMINATING = (
        "ENGINE_STATUS_RUNNING_REVISIONS_TERMINATING"
    )
    """ Engine is initialized;
    all child revisions are being terminated."""

    # Engine termination request was sent.
    ENGINE_STATUS_TERMINATION_PENDING = "ENGINE_STATUS_TERMINATION_PENDING"
    """ Engine termination request was sent."""

    ENGINE_STATUS_TERMINATION_ST = "ENGINE_STATUS_TERMINATION_STARTED"
    """ Engine termination started."""

    ENGINE_STATUS_TERMINATION_FIN = "ENGINE_STATUS_TERMINATION_FINISHED"
    """ Engine termination finished."""

    ENGINE_STATUS_TERMINATION_F = "ENGINE_STATUS_TERMINATION_FAILED"
    """ Engine termination failed."""

    ENGINE_STATUS_DELETED = "ENGINE_STATUS_DELETED"
    """ Engine is soft-deleted."""


class EngineStatusSummary(Enum):
    """
    Engine summary status.

    See: https://api.dev.firebolt.io/devDocs#operation/coreV1GetEngine
    """

    ENGINE_STATUS_SUMMARY_UNSPECIFIED = "ENGINE_STATUS_SUMMARY_UNSPECIFIED"
    """Status unspecified"""

    ENGINE_STATUS_SUMMARY_STOPPED = "ENGINE_STATUS_SUMMARY_STOPPED"
    """ Fully stopped."""

    ENGINE_STATUS_SUMMARY_STARTING = "ENGINE_STATUS_SUMMARY_STARTING"
    """ Provisioning process is in progress;
     creating cloud infra for this engine."""

    ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING = (
        "ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING"
    )
    """ Provisioning process is complete;
     waiting for PackDB cluster to initialize and start."""

    ENGINE_STATUS_SUMMARY_RUNNING = "ENGINE_STATUS_SUMMARY_RUNNING"
    """ Fully started;
     engine is ready to serve requests."""

    ENGINE_STATUS_SUMMARY_UPGRADING = "ENGINE_STATUS_SUMMARY_UPGRADING"
    """ Version of the PackDB is changing.
     This is zero downtime operation that does not affect engine work.
     This status is reserved for future use (not used fow now)."""

    ENGINE_STATUS_SUMMARY_RESTARTING = "ENGINE_STATUS_SUMMARY_RESTARTING"
    """ Hard restart (full stop/start cycle) is in progress.
    Underlying infrastructure is being recreated."""

    ENGINE_STATUS_SUMMARY_RESTARTING_INITIALIZING = (
        "ENGINE_STATUS_SUMMARY_RESTARTING_INITIALIZING"
    )
    """ Hard restart (full stop/start cycle) is in progress.
     Underlying infrastructure is ready. Waiting for
     PackDB cluster to initialize and start.
     This status is logically the same as ENGINE_STATUS_SUMMARY_STARTING_INITIALIZING,
     but used during restart cycle."""

    ENGINE_STATUS_SUMMARY_REPAIRING = "ENGINE_STATUS_SUMMARY_REPAIRING"
    """ Underlying infrastructure has issues and is being repaired.
     Engine is still running, but it's not fully healthy and some queries may fail."""

    ENGINE_STATUS_SUMMARY_STOPPING = "ENGINE_STATUS_SUMMARY_STOPPING"
    """ Stop is in progress."""

    ENGINE_STATUS_SUMMARY_DELETING = "ENGINE_STATUS_SUMMARY_DELETING"
    """ Termination is in progress.
     All infrastructure that belongs to this engine will be completely destroyed."""

    ENGINE_STATUS_SUMMARY_DELETED = "ENGINE_STATUS_SUMMARY_DELETED"
    """ Infrastructure is terminated, engine data is deleted."""

    ENGINE_STATUS_SUMMARY_FAILED = "ENGINE_STATUS_SUMMARY_FAILED"
    """ Failed to start or stop.
     This status only indicates that there were issues during provisioning operations.
     If engine enters this status,
     all infrastructure should be stopped/terminated already."""


class EngineOrder(Enum):
    ENGINE_ORDER_UNSPECIFIED = "ENGINE_ORDER_UNSPECIFIED"
    ENGINE_ORDER_NAME_ASC = "ENGINE_ORDER_NAME_ASC"
    ENGINE_ORDER_NAME_DESC = "ENGINE_ORDER_NAME_DESC"
    ENGINE_ORDER_COMPUTE_REGION_ID_ASC = "ENGINE_ORDER_COMPUTE_REGION_ID_ASC"
    ENGINE_ORDER_COMPUTE_REGION_ID_DESC = "ENGINE_ORDER_COMPUTE_REGION_ID_DESC"
    ENGINE_ORDER_CURRENT_STATUS_ASC = "ENGINE_ORDER_CURRENT_STATUS_ASC"
    ENGINE_ORDER_CURRENT_STATUS_DESC = "ENGINE_ORDER_CURRENT_STATUS_DESC"
    ENGINE_ORDER_CREATE_TIME_ASC = "ENGINE_ORDER_CREATE_TIME_ASC"
    ENGINE_ORDER_CREATE_TIME_DESC = "ENGINE_ORDER_CREATE_TIME_DESC"
    ENGINE_ORDER_CREATE_ACTOR_ASC = "ENGINE_ORDER_CREATE_ACTOR_ASC"
    ENGINE_ORDER_CREATE_ACTOR_DESC = "ENGINE_ORDER_CREATE_ACTOR_DESC"
    ENGINE_ORDER_LAST_UPDATE_TIME_ASC = "ENGINE_ORDER_LAST_UPDATE_TIME_ASC"
    ENGINE_ORDER_LAST_UPDATE_TIME_DESC = "ENGINE_ORDER_LAST_UPDATE_TIME_DESC"
    ENGINE_ORDER_LAST_UPDATE_ACTOR_ASC = "ENGINE_ORDER_LAST_UPDATE_ACTOR_ASC"
    ENGINE_ORDER_LAST_UPDATE_ACTOR_DESC = "ENGINE_ORDER_LAST_UPDATE_ACTOR_DESC"
    ENGINE_ORDER_LATEST_REVISION_CURRENT_STATUS_ASC = (
        "ENGINE_ORDER_LATEST_REVISION_CURRENT_STATUS_ASC"
    )
    ENGINE_ORDER_LATEST_REVISION_CURRENT_STATUS_DESC = (
        "ENGINE_ORDER_LATEST_REVISION_CURRENT_STATUS_DESC"
    )
    ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_COUNT_ASC = (
        "ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_COUNT_ASC"
    )
    ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_COUNT_DESC = (
        "ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_COUNT_DESC"
    )
    ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_TYPE_ID_ASC = (
        "ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_TYPE_ID_ASC"
    )
    ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_TYPE_ID_DESC = (
        "ENGINE_ORDER_LATEST_REVISION_SPECIFICATION_DB_COMPUTE_INSTANCES_TYPE_ID_DESC"
    )


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
