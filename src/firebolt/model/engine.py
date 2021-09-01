from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

from pydantic import Field
from toolz import first

from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.database import Database
from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey
from firebolt.model.region import RegionKey, regions

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
    def analytics_default(cls) -> EngineSettings:
        """Default settings for the data analytics (querying) use case."""
        return cls(
            preset="ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=True,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )

    @classmethod
    def ingest_default(cls) -> EngineSettings:
        """Default settings for the data ingestion use case."""
        return cls(
            preset="ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=False,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )


class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, data ingestion).

    Engines are configured in Settings and in EngineRevisions.
    """

    name: str
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
    def get_by_id(cls, engine_id: str) -> Engine:
        """Get an Engine from Firebolt by its id."""
        firebolt_client = cls.get_firebolt_client()
        response = firebolt_client.http_client.get(
            url=f"/core/v1/accounts/{firebolt_client.account_id}/engines/{engine_id}",
        )
        engine_spec: dict = response.json()["engine"]
        return cls.parse_obj(engine_spec)

    @classmethod
    def get_by_name(cls, engine_name: str) -> Engine:
        """Get an Engine from Firebolt by its name."""
        response = cls.get_firebolt_client().http_client.get(
            url="/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return cls.get_by_id(engine_id=engine_id)

    @classmethod
    def create_analytics(
        cls,
        name: str,
        description: Optional[str] = None,
        region_name: Optional[str] = None,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> Engine:
        """
        Create a new engine on Firebolt, based on default Analytics settings.

        (The engine should be used for running queries on Firebolt.)

        Args:
            name: Name of the engine.
            description: Long description of the engine.
            region_name: Name of the region in which to create the engine.
                If omitted, use the default region.
            compute_instance_type_name: Name of the instance type to use for the Engine.
            compute_instance_count: Number of instances to use for the Engine.

        Returns:
            The newly created engine.
        """
        engine = cls._default(
            name=name,
            settings=EngineSettings.analytics_default(),
            description=description,
            region_name=region_name,
        )
        return engine.apply_create(
            engine_revision=EngineRevision.analytics_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    @classmethod
    def create_ingest(
        cls,
        name: str,
        description: Optional[str] = None,
        region_name: Optional[str] = None,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> Engine:
        """
        Create a new engine on Firebolt, based on default Ingest settings.

        (The engine should be used for ingesting data into Firebolt.)

        Args:
            name: Name of the engine.
            description: Long description of the engine.
            region_name: Name of the region in which to create the engine.
                If omitted, use the default region.
            compute_instance_type_name: Name of the instance type to use for the Engine.
            compute_instance_count: Number of instances to use for the Engine.
        Returns:
            The newly created engine.
        """
        engine = cls._default(
            name=name,
            settings=EngineSettings.ingest_default(),
            description=description,
            region_name=region_name,
        )
        return engine.apply_create(
            engine_revision=EngineRevision.ingest_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    @classmethod
    def _default(
        cls,
        name: str,
        settings: EngineSettings,
        description: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> Engine:
        """
        Create a new local Engine object with default settings.

        Args:
            name: Name of the engine.
            settings: Engine revision settings to apply to the engine.
            description: Description of the engine.
            region_name: Region in which to create the engine.

        Returns:
            The new local Engine object.
        """
        if region_name is not None:
            region = regions.get_by_name(region_name=region_name)
        else:
            region = regions.default_region
        return Engine(
            name=name,
            description=description,
            compute_region_key=region.key,
            settings=settings,
        )

    @classmethod
    def get_by_ids(cls, engine_ids: list[str]) -> list[Engine]:
        """Get multiple Engines from Firebolt by their ids."""
        fc = cls.get_firebolt_client()
        response = fc.http_client.post(
            url=f"/core/v1/engines:getByIds",
            json={
                "engine_ids": [
                    {"account_id": fc.account_id, "engine_id": engine_id}
                    for engine_id in engine_ids
                ]
            },
        )
        return [cls.parse_obj(e) for e in response.json()["engines"]]

    @property
    def engine_id(self) -> Optional[str]:
        if self.key is None:
            return None
        return self.key.engine_id

    @property
    def database(self) -> Optional[Database]:
        """The database the engine is bound to, if any."""
        # FUTURE: in the new architecture, an engine can be bound to multiple databases
        try:
            binding = first(Binding.list_bindings(engine_id=self.engine_id))
            return Database.get_by_id(binding.database_id)
        except StopIteration:
            return None

    def apply_create(self, engine_revision: Optional[EngineRevision] = None) -> Engine:
        """
        Create a new Engine on Firebolt from the local Engine object.

        Args:
            engine_revision: EngineRevision to use for configuring the Engine.

        Returns:
            The newly created engine.
        """

        json_payload = _EngineCreateRequest(
            account_id=self.get_firebolt_client().account_id,
            engine=self,
            engine_revision=engine_revision,
        ).json(by_alias=True)

        response = self.get_firebolt_client().http_client.post(
            url="/core/v1/account/engines",
            headers={"Content-type": "application/json"},
            data=json_payload,
        )
        return Engine.parse_obj(response.json()["engine"])

    def apply_create_with_latest_revision(self) -> Engine:
        """
        Create a new Engine on Firebolt from the local Engine object,
        passing in the latest EngineRevision from Firebolt.

        Note: this will only work on Engines that already exist on Firebolt.
        This method is mainly useful for copying existing engines.
        """
        engine_revision = self.get_latest_engine_revision()
        if engine_revision is None:
            raise RuntimeError("An EngineRevision does not exist for this Engine")
        return self.apply_create(engine_revision=engine_revision)

    def get_latest_engine_revision(self) -> Optional[EngineRevision]:
        """Get the latest engine revision, if one exists."""
        if self.latest_revision_key is None:
            return None
        return EngineRevision.get_by_engine_revision_key(
            engine_revision_key=self.latest_revision_key
        )

    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        print_dots=True,
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
        if self.engine_id is None:
            raise ValueError("engine_id must be set before starting")
        response = self.get_firebolt_client().http_client.post(
            url=f"/core/v1/account/engines/{self.engine_id}:start",
        )
        engine = Engine.parse_obj(response.json()["engine"])
        status = engine.current_status_summary
        logger.info(
            f"Starting Engine engine_id={self.engine_id} "
            f"name={self.name} status_summary={status}"
        )
        start_time = time.time()
        end_time = start_time + wait_timeout_seconds

        # summary statuses: https://tinyurl.com/as7a9ru9
        while wait_for_startup and status != "ENGINE_STATUS_SUMMARY_RUNNING":
            if time.time() >= end_time:
                raise TimeoutError(
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                )
            engine = self.get_by_id(engine_id=self.engine_id)
            new_status = engine.current_status_summary
            if new_status != status:
                logger.info(f"Engine status_summary={new_status}")
            elif print_dots:
                print(".", end="")
            time.sleep(5)
            status = new_status
        return engine

    def delete(self) -> str:
        """Delete an Engine from Firebolt."""
        firebolt_client = self.get_firebolt_client()
        response = firebolt_client.http_client.delete(
            url=f"/core/v1"
            f"/accounts/{firebolt_client.account_id}"
            f"/engines/{self.engine_id}",
        )
        return response.json()


class _EngineCreateRequest(FireboltBaseModel):
    """Helper model for sending Engine create requests."""

    account_id: str
    engine: Engine
    engine_revision: Optional[EngineRevision]
