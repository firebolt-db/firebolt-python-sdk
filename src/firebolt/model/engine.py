from __future__ import annotations

import functools
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Callable, Optional

from pydantic import Field, PrivateAttr

from firebolt.common.exception import NoAttachedDatabaseError
from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.database import Database
from firebolt.model.engine_revision import EngineRevisionKey
from firebolt.model.region import RegionKey
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)

if TYPE_CHECKING:
    from firebolt.service.engine import EngineService

logger = logging.getLogger(__name__)


class EngineKey(FireboltBaseModel):
    account_id: str
    engine_id: str


class EngineSettings(FireboltBaseModel):
    """
    Engine Settings.

    See Also: engine_revision.Specification which also contains engine configuration.
    """

    preset: str
    auto_stop_delay_duration: str
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def default(
        cls,
        engine_type: EngineType = EngineType.GENERAL_PURPOSE,
        auto_stop_delay_duration: str = "1200s",
        warm_up: WarmupMethod = WarmupMethod.PRELOAD_INDEXES,
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
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
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


class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, data ingestion).

    Engines are configured in Settings and in EngineRevisions.
    """

    # internal
    _engine_service: EngineService = PrivateAttr()

    # required
    name: Annotated[str, Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")]
    compute_region_key: RegionKey = Field(alias="compute_region_id")
    settings: EngineSettings

    # optional
    key: Optional[EngineKey] = Field(alias="id")
    description: Optional[str]
    emoji: Optional[str]
    current_status: Optional[str]
    current_status_summary: Optional[str]
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
        engine._engine_service = engine_service
        return engine

    @property
    def engine_id(self) -> str:
        if self.key is None:
            raise ValueError("engine key is None")
        return self.key.engine_id

    @property
    def database(self) -> Optional[Database]:
        return (
            self._engine_service.resource_manager.bindings.get_database_bound_to_engine(
                engine=self
            )
        )

    @property
    def url(self) -> Optional[str]:
        return self.endpoint

    def attach_to_database(
        self, database: Database, is_default_engine: bool = False
    ) -> Binding:
        """
        Attach this engine to a database.

        Args:
            database: Database to which the engine will be attached.
            is_default_engine:
                Whether this engine should be used as default for this database.
                Only one engine can be set as default for a single database.
                This will overwrite any existing default.
        """
        return self._engine_service.resource_manager.bindings.create_binding(
            engine=self, database=database, is_default_engine=is_default_engine
        )

    @check_attached_to_database
    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        print_dots: bool = True,
    ) -> Engine:
        """
        Start an engine. If it's already started, do nothing.

        Args:
            wait_for_startup:
                If True, wait for startup to complete.
                If false, return immediately after requesting startup.
            wait_timeout_seconds:
                Number of seconds to wait for startup to complete
                before raising a TimeoutError.
            print_dots:
                If True, print dots periodically while waiting for engine startup.
                If false, do not print any dots.

        Returns:
            The updated Engine from Firebolt.
        """
        response = self._engine_service.client.post(
            url=f"/core/v1/account/engines/{self.engine_id}:start",
        )
        engine = Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._engine_service
        )
        status = engine.current_status_summary
        logger.info(
            f"Starting Engine engine_id={engine.engine_id} "
            f"name={engine.name} status_summary={status}"
        )
        start_time = time.time()
        end_time = start_time + wait_timeout_seconds

        while (
            wait_for_startup
            and status != EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING.name
        ):
            if time.time() >= end_time:
                raise TimeoutError(
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                )
            engine = self._engine_service.get(engine_id=engine.engine_id)
            new_status = engine.current_status_summary
            if new_status != status:
                logger.info(f"Engine status_summary={new_status}")
            elif print_dots:
                print(".", end="")
            time.sleep(5)
            status = new_status
        return engine

    @check_attached_to_database
    def stop(self, engine: Engine) -> Engine:
        """Stop an Engine running on Firebolt."""
        response = self._engine_service.client.post(
            url=f"/core/v1/account/engines/{engine.engine_id}:stop",
        )
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._engine_service
        )

    def delete(self, engine: Engine) -> Engine:
        """Delete an Engine from Firebolt."""
        response = self._engine_service.client.delete(
            url=f"/core/v1"
            f"/accounts/{self._engine_service.account_id}"
            f"/engines/{engine.engine_id}",
        )
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._engine_service
        )
