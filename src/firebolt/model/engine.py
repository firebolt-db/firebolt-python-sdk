from __future__ import annotations

import functools
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

from pydantic import Field, PrivateAttr

from firebolt.db import Connection, connect
from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.database import Database
from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey
from firebolt.model.region import RegionKey
from firebolt.service.types import (
    EngineStatus,
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)
from firebolt.utils.exception import NoAttachedDatabaseError
from firebolt.utils.urls import (
    ACCOUNT_ENGINE_RESTART_URL,
    ACCOUNT_ENGINE_START_URL,
    ACCOUNT_ENGINE_STOP_URL,
    ACCOUNT_ENGINE_URL,
)
from firebolt.utils.util import prune_dict

if TYPE_CHECKING:
    from firebolt.service.engine import EngineService

logger = logging.getLogger(__name__)


class EngineKey(FireboltBaseModel):
    account_id: str
    engine_id: str


def wait(seconds: int, timeout_time: float, error_message: str, verbose: bool) -> None:
    time.sleep(seconds)
    if time.time() > timeout_time:
        raise TimeoutError(error_message)
    if verbose:
        print(".", end="")


class EngineSettings(FireboltBaseModel):
    """
    Engine settings.

    See also: :py:class:`EngineRevisionSpecification
    <firebolt.model.engine_revision.EngineRevisionSpecification>`
    which also contains engine configuration.
    """

    preset: str
    auto_stop_delay_duration: str = Field(regex=r"^[0-9]+[sm]$|^0$")
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def default(
        cls,
        engine_type: EngineType = EngineType.GENERAL_PURPOSE,
        auto_stop_delay_duration: str = "1200s",
        warm_up: WarmupMethod = WarmupMethod.PRELOAD_INDEXES,
        minimum_logging_level: str = "ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
    ) -> EngineSettings:
        if engine_type == EngineType.GENERAL_PURPOSE:
            preset = engine_type.GENERAL_PURPOSE.api_settings_preset_name  # type: ignore # noqa: E501
            is_read_only = False
        else:
            preset = engine_type.DATA_ANALYTICS.api_settings_preset_name  # type: ignore
            is_read_only = True

        return cls(
            preset=preset,
            auto_stop_delay_duration=auto_stop_delay_duration,
            minimum_logging_level=minimum_logging_level,
            is_read_only=is_read_only,
            warm_up=warm_up.api_name,
        )


def check_attached_to_database(func: Callable) -> Callable:
    """(Decorator) Ensure the engine is attached to a database."""

    @functools.wraps(func)
    def inner(self: Engine, *args: Any, **kwargs: Any) -> Any:
        if self.database is None:
            raise NoAttachedDatabaseError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


class FieldMask(FireboltBaseModel):
    paths: Sequence[str] = Field(alias="paths")


class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, ingestion).

    Engines are configured in :py:class:`Settings
    <firebolt.model.engine.EngineSettings>`
    and in :py:class:`EngineRevisionSpecification
    <firebolt.model.engine_revision.EngineRevisionSpecification>`.
    """

    # internal
    _service: EngineService = PrivateAttr()

    # required
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    compute_region_key: RegionKey = Field(alias="compute_region_id")
    settings: EngineSettings

    # optional
    key: Optional[EngineKey] = Field(alias="id")
    description: Optional[str]
    emoji: Optional[str]
    current_status: Optional[EngineStatus]
    current_status_summary: Optional[EngineStatusSummary]
    latest_revision_key: Optional[EngineRevisionKey] = Field(alias="latest_revision_id")
    endpoint: Optional[str]
    endpoint_serving_revision_key: Optional[EngineRevisionKey] = Field(
        alias="endpoint_serving_revision_id"
    )
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    last_use_time: Optional[datetime]
    desired_status: Optional[str]
    health_status: Optional[str]
    endpoint_desired_revision_key: Optional[EngineRevisionKey] = Field(
        alias="endpoint_desired_revision_id"
    )

    @classmethod
    def parse_obj_with_service(cls, obj: Any, engine_service: EngineService) -> Engine:
        engine = cls.parse_obj(obj)
        engine._service = engine_service
        return engine

    @property
    def engine_id(self) -> str:
        if self.key is None:
            raise ValueError("engine key is None")
        return self.key.engine_id

    @property
    def database(self) -> Optional[Database]:
        return self._service.resource_manager.bindings.get_database_bound_to_engine(
            engine=self
        )

    def get_latest(self) -> Engine:
        """Get an up-to-date instance of the engine from Firebolt."""
        return self._service.get(id_=self.engine_id)

    def attach_to_database(
        self, database: Database, is_default_engine: bool = False
    ) -> Binding:
        """
        Attach this engine to a database.

        Args:
            database: Database to which the engine will be attached
            is_default_engine:
                Whether this engine should be used as default for this database.
                Only one engine can be set as default for a single database.
                This will overwrite any existing default.
        """
        return self._service.resource_manager.bindings.create(
            engine=self, database=database, is_default_engine=is_default_engine
        )

    @check_attached_to_database
    def get_connection(self) -> Connection:
        """Get a connection to the attached database for running queries.

        Returns:
            firebolt.db.connection.Connection: engine connection instance

        """
        return connect(
            database=self.database.name,  # type: ignore # already checked by decorator
            auth=self._service.client.auth,
            engine_url=self.endpoint,
            account_name=self._service.settings.account_name,
            api_endpoint=self._service.settings.server,
        )

    @check_attached_to_database
    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        verbose: bool = False,
    ) -> Engine:
        """
        Start an engine. If it's already started, do nothing.

        Args:
            wait_for_startup:
                If True, wait for startup to complete.
                If False, return immediately after requesting startup.
            wait_timeout_seconds:
                Number of seconds to wait for startup to complete
                before raising a TimeoutError
            verbose:
                If True, print dots periodically while waiting for engine start.
                If False, do not print any dots.

        Returns:
            The updated engine from Firebolt.
        """
        timeout_time = time.time() + wait_timeout_seconds

        engine = self.get_latest()
        if (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
        ):
            logger.info(
                f"Engine (engine_id={self.engine_id}, name={self.name}) "
                "is already running."
            )
            return engine

        # wait for engine to stop first, if it's already stopping
        # FUTURE: revisit logging and consider consolidating this if & the while below.
        elif (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING
        ):
            logger.info(
                f"Engine (engine_id={engine.engine_id}, name={engine.name}) "
                "is in currently stopping, waiting for it to stop first."
            )
            while (
                engine.current_status_summary
                != EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
            ):
                wait(
                    seconds=5,
                    timeout_time=timeout_time,
                    error_message=(
                        "Engine "
                        f"(engine_id={engine.engine_id}, name={engine.name}) "
                        f"did not stop within {wait_timeout_seconds} seconds."
                    ),
                    verbose=True,
                )
                engine = engine.get_latest()

            logger.info(
                f"Engine (engine_id={engine.engine_id}, name={engine.name}) stopped."
            )

        engine = self._send_engine_request(ACCOUNT_ENGINE_START_URL)
        logger.info(
            f"Starting Engine (engine_id={engine.engine_id}, name={engine.name})"
        )

        # wait for engine to start
        while wait_for_startup and engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:
            wait(
                seconds=5,
                timeout_time=timeout_time,
                error_message=(  # noqa: E501
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                ),
                verbose=verbose,
            )
            previous_status_summary = engine.current_status_summary
            engine = engine.get_latest()
            if engine.current_status_summary != previous_status_summary:
                logger.info(
                    "Engine status_summary="
                    f"{getattr(engine.current_status_summary, 'name')}"
                )

        return engine

    @check_attached_to_database
    def stop(
        self, wait_for_stop: bool = False, wait_timeout_seconds: int = 3600
    ) -> Engine:
        """Stop an Engine running on Firebolt."""
        timeout_time = time.time() + wait_timeout_seconds

        engine = self._send_engine_request(ACCOUNT_ENGINE_STOP_URL)
        logger.info(f"Stopping Engine (engine_id={self.engine_id}, name={self.name})")

        while wait_for_stop and engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:
            wait(
                seconds=5,
                timeout_time=timeout_time,
                error_message=(  # noqa: E501
                    f"Could not stop engine within {wait_timeout_seconds} seconds."
                ),
                verbose=False,
            )

            engine = engine.get_latest()

        return engine

    def update(
        self,
        name: Optional[str] = None,
        engine_type: Optional[EngineType] = None,
        scale: Optional[int] = None,
        spec: Optional[str] = None,
        auto_stop: Optional[int] = None,
        warmup: Optional[WarmupMethod] = None,
        description: Optional[str] = None,
        use_spot: Optional[bool] = None,
    ) -> Engine:
        """
        Updates the engine and returns an updated version of the engine. If all
        parameters are set to None, old engine parameter values remain.
        """

        class _EngineUpdateRequest(FireboltBaseModel):
            """Helper model for sending engine update requests."""

            account_id: str
            desired_revision: Optional[EngineRevision]
            engine: Engine
            engine_id: str
            update_mask: FieldMask

        # Update the engine parameters
        self.name = name if name else self.name
        self.description = description

        # Update engine settings
        engine_settings_params = {
            "engine_type": engine_type,
            "auto_stop_delay_duration": f"{auto_stop * 60}s"
            if auto_stop is not None
            else None,
            "warm_up": warmup,
        }
        self.settings = EngineSettings.default(**prune_dict(engine_settings_params))

        # Update the engine desired_revision if needed
        desired_revision = None
        if (scale or spec or use_spot is not None) and self.latest_revision_key:
            rm = self._service.resource_manager
            desired_revision = rm.engine_revisions.get_by_key(self.latest_revision_key)

            if spec:
                instance_type_key = rm.instance_types.get_by_name(
                    instance_type_name=spec,
                    region_name=rm.regions.regions_by_key[self.compute_region_key].name,
                ).key

                desired_revision.specification.db_compute_instances_type_key = (
                    instance_type_key
                )
                desired_revision.specification.proxy_instances_type_key = (
                    instance_type_key
                )

            if scale:
                desired_revision.specification.db_compute_instances_count = scale

            if use_spot is not None:
                desired_revision.specification.db_compute_instances_use_spot = use_spot

        update_mask_paths = list(
            prune_dict(
                {
                    "name": name,
                    "description": description,
                    "settings.auto_stop_delay_duration": auto_stop,
                    "settings.warm_up": warmup,
                    "settings.is_read_only": engine_type,
                    "settings.preset": engine_type,
                }
            ).keys()
        )

        # Send the update request
        response = self._service.client.patch(
            url=ACCOUNT_ENGINE_URL.format(
                engine_id=self.engine_id, account_id=self._service.account_id
            ),
            headers={"Content-type": "application/json"},
            json=_EngineUpdateRequest(
                account_id=self._service.account_id,
                engine=self,
                desired_revision=desired_revision,
                engine_id=self.engine_id,
                update_mask=FieldMask(paths=update_mask_paths),
            ).jsonable_dict(by_alias=True),
        )

        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._service
        )

    @check_attached_to_database
    def restart(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
    ) -> Engine:
        """
        Restart an engine.

        Args:
            wait_for_startup:
                If True, wait for startup to complete.
                If False, return immediately after requesting startup.
            wait_timeout_seconds:
                Number of seconds to wait for startup to complete
                before raising a TimeoutError.

        Returns:
            The updated engine from Firebolt.
        """
        timeout_time = time.time() + wait_timeout_seconds

        engine = self._send_engine_request(ACCOUNT_ENGINE_RESTART_URL)
        logger.info(f"Stopping Engine (engine_id={self.engine_id}, name={self.name})")

        while wait_for_startup and engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:
            wait(
                seconds=5,
                timeout_time=timeout_time,
                error_message=(  # noqa: E501
                    f"Could not restart engine within {wait_timeout_seconds} seconds."
                ),
                verbose=False,
            )

            engine = engine.get_latest()

        return engine

    def delete(self) -> Engine:
        """Delete an engine from Firebolt."""
        response = self._service.client.delete(
            url=ACCOUNT_ENGINE_URL.format(
                account_id=self._service.account_id, engine_id=self.engine_id
            ),
        )
        logger.info(f"Deleting Engine (engine_id={self.engine_id}, name={self.name})")
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._service
        )

    def _send_engine_request(self, url: str) -> Engine:
        response = self._service.client.post(
            url=url.format(
                account_id=self._service.account_id, engine_id=self.engine_id
            )
        )
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._service
        )


class _EngineCreateRequest(FireboltBaseModel):
    """Helper model for sending engine create requests."""

    account_id: str
    engine: Engine
    engine_revision: Optional[EngineRevision]
