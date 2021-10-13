import json
import logging
from typing import Optional, Union

from firebolt.model import FireboltBaseModel
from firebolt.model.engine import Engine, EngineSettings
from firebolt.model.engine_revision import (
    EngineRevision,
    EngineRevisionSpecification,
)
from firebolt.model.region import Region
from firebolt.service.base import BaseService
from firebolt.service.types import EngineType, WarmupMethod

logger = logging.getLogger(__name__)


class EngineService(BaseService):
    def parse_engine_dict(self, engine_dict: dict) -> Engine:
        engine = Engine.parse_obj(engine_dict)
        engine._engine_service = self
        return engine

    def get_by_id(self, engine_id: str) -> Engine:
        """Get an Engine from Firebolt by its id."""
        response = self.client.get(
            url=f"/core/v1/accounts/{self.account_id}/engines/{engine_id}",
        )
        engine_entry: dict = response.json()["engine"]
        return self.parse_engine_dict(engine_entry)

    def get_by_ids(self, engine_ids: list[str]) -> list[Engine]:
        """Get multiple Engines from Firebolt by their ids."""
        response = self.client.post(
            url=f"/core/v1/engines:getByIds",
            json={
                "engine_ids": [
                    {"account_id": self.account_id, "engine_id": engine_id}
                    for engine_id in engine_ids
                ]
            },
        )
        return [self.parse_engine_dict(e) for e in response.json()["engines"]]

    def get_by_name(self, engine_name: str) -> Engine:
        """Get an Engine from Firebolt by its name."""
        response = self.client.get(
            url="/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return self.get_by_id(engine_id=engine_id)

    def create(
        self,
        name: str,
        region: Union[str, Region, None] = None,
        engine_type: Union[str, EngineType] = EngineType.GENERAL_PURPOSE,
        scale: int = 2,
        spec: str = "i3.4xlarge",
        auto_stop: int = 20,
        warmup: Union[str, WarmupMethod] = WarmupMethod.PRELOAD_INDEXES,
        description: str = "",
    ) -> Engine:
        """
        Create a new Engine.

        Args:
            name: An identifier that specifies the name of the engine.
            region: The AWS region in which the engine runs.
            engine_type: The engine type. GENERAL_PURPOSE or DATA_ANALYTICS
            scale: The number of compute instances on the engine.
                The scale can be any int from 1 to 128.
            spec: The AWS EC2 instance type.
            auto_stop: The amount of time (in minutes) after which
                the engine automatically stops.
            warmup: The warmup method that should be used.
                MINIMAL: On-demand loading (both indexes and tables' data).
                PRELOAD_INDEXES: Load indexes only.
                PRELOAD_ALL_DATA: Full data auto-load
                    (both indexes and table data - full warmup).
            description: A short description of the engine's purpose.

        Returns:
            Engine with the specified settings.
        """
        if isinstance(engine_type, str):
            engine_type = EngineType[engine_type]
        if isinstance(warmup, str):
            warmup = WarmupMethod[warmup]

        if region is None:
            region = self.resource_manager.regions.default_region
        else:
            if isinstance(region, str):
                region = self.resource_manager.regions.get_by_name(region_name=region)

        engine = Engine(
            name=name,
            description=description,
            compute_region_key=region.key,
            settings=EngineSettings.default(
                engine_type=engine_type,
                auto_stop_delay_duration=f"{auto_stop * 60}s",
                warm_up=warmup,
            ),
        )

        instance_type_key = self.resource_manager.instance_types.get_by_name(
            instance_type_name=spec
        ).key

        engine_revision = EngineRevision(
            specification=EngineRevisionSpecification(
                db_compute_instances_type_key=instance_type_key,
                db_compute_instances_count=scale,
                db_compute_instances_use_spot=False,
                db_version="",
                proxy_instances_type_key=instance_type_key,
                proxy_instances_count=1,
                proxy_version="",
            )
        )

        return self._send_create_engine(engine=engine, engine_revision=engine_revision)

    # def attach_to_database(
    #     self, engine: Engine, database: Database, is_default_engine: bool
    # ) -> Binding:
    #     """
    #     Attach this engine to a database.
    #
    #     Args:
    #         engine: Engine to attach to the database.
    #         database: Database to which the engine will be attached.
    #         is_default_engine:
    #             Whether this engine should be used as default for this database.
    #             Only one engine can be set as default for a single database.
    #             This will overwrite any existing default.
    #     """
    #     return self.resource_manager.bindings.create_binding(
    #         engine=engine, database=database, is_default_engine=is_default_engine
    #     )

    def _send_create_engine(
        self, engine: Engine, engine_revision: Optional[EngineRevision] = None
    ) -> Engine:
        """
        Create a new Engine on Firebolt from the local Engine object.

        Args:
            engine: The Engine to create.
            engine_revision: EngineRevision to use for configuring the Engine.

        Returns:
            The newly created engine.
        """

        class _EngineCreateRequest(FireboltBaseModel):
            """Helper model for sending Engine create requests."""

            account_id: str
            engine: Engine
            engine_revision: Optional[EngineRevision]

        response = self.client.post(
            url="/core/v1/account/engines",
            headers={"Content-type": "application/json"},
            json=json.loads(
                _EngineCreateRequest(
                    account_id=self.account_id,
                    engine=engine,
                    engine_revision=engine_revision,
                ).json(by_alias=True)
            ),
        )
        return self.parse_engine_dict(response.json()["engine"])

    # def start(
    #     self,
    #     engine: Engine,
    #     wait_for_startup: bool = True,
    #     wait_timeout_seconds: int = 3600,
    #     print_dots: bool = True,
    # ) -> Engine:
    #     """
    #     Start an engine. If it's already started, do nothing.
    #
    #     Args:
    #         engine:
    #             The engine to start.
    #         wait_for_startup:
    #             If True, wait for startup to complete.
    #             If false, return immediately after requesting startup.
    #         wait_timeout_seconds:
    #             Number of seconds to wait for startup to complete
    #             before raising a TimeoutError.
    #         print_dots:
    #             If True, print dots periodically while waiting for engine startup.
    #             If false, do not print any dots.
    #
    #     Returns:
    #         The updated Engine from Firebolt.
    #     """
    #     response = self.client.post(
    #         url=f"/core/v1/account/engines/{engine.engine_id}:start",
    #     )
    #     engine = Engine.parse_obj(response.json()["engine"])
    #     status = engine.current_status_summary
    #     logger.info(
    #         f"Starting Engine engine_id={engine.engine_id} "
    #         f"name={engine.name} status_summary={status}"
    #     )
    #     start_time = time.time()
    #     end_time = start_time + wait_timeout_seconds
    #
    #     # summary statuses: https://tinyurl.com/as7a9ru9
    #     while wait_for_startup and status != "ENGINE_STATUS_SUMMARY_RUNNING":
    #         if time.time() >= end_time:
    #             raise TimeoutError(
    #                 f"Could not start engine within {wait_timeout_seconds} seconds."
    #             )
    #         engine = self.get_by_id(engine_id=engine.engine_id)
    #         new_status = engine.current_status_summary
    #         if new_status != status:
    #             logger.info(f"Engine status_summary={new_status}")
    #         elif print_dots:
    #             print(".", end="")
    #         time.sleep(5)
    #         status = new_status
    #     return engine

    # def stop(self, engine: Engine) -> Engine:
    #     """Stop an Engine running on Firebolt."""
    #     response = self.client.post(
    #         url=f"/core/v1/account/engines/{engine.engine_id}:stop",
    #     )
    #     return self.parse_engine_dict(response.json()["engine"])
    #
    # def delete(self, engine: Engine) -> Engine:
    #     """Delete an Engine from Firebolt."""
    #     response = self.client.delete(
    #         url=f"/core/v1"
    #         f"/accounts/{self.account_id}"
    #         f"/engines/{engine.engine_id}",
    #     )
    #     return self.parse_engine_dict(response.json()["engine"])
