import logging
import time
from typing import Optional

from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.database import Database
from firebolt.model.engine import Engine, EngineRevision, EngineSettings
from firebolt.model.engine_revision import EngineRevisionKey
from firebolt.model.region import regions
from firebolt.service.base_service import BaseService
from firebolt.service.binding_service import BindingService

logger = logging.getLogger(__name__)


class EngineService(BaseService):
    def get_engine_by_id(self, engine_id: str) -> Engine:
        """Get an Engine from Firebolt by its id."""
        response = self.firebolt_client.get(
            url=f"/core/v1/accounts/{self.account_id}/engines/{engine_id}",
        )
        engine_spec: dict = response.json()["engine"]
        return Engine.parse_obj(engine_spec)

    def get_engine_by_name(self, engine_name: str) -> Engine:
        """Get an Engine from Firebolt by its name."""
        response = self.firebolt_client.get(
            url="/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return self.get_engine_by_id(engine_id=engine_id)

    def get_engine_revision_by_id(
        self, engine_id: str, engine_revision_id: str
    ) -> EngineRevision:
        """Get an EngineRevision from Firebolt by engine_id and engine_revision_id."""
        return self.get_engine_revision_by_key(
            EngineRevisionKey(
                account_id=self.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    def get_engine_revision_by_key(
        self, engine_revision_key: EngineRevisionKey
    ) -> EngineRevision:
        """
        Fetch an EngineRevision from Firebolt by it's key.

        Args:
            engine_revision_key: Key of the desired EngineRevision.

        Returns:
            The requested EngineRevision
        """
        response = self.firebolt_client.get(
            url=f"/core/v1/accounts/{engine_revision_key.account_id}"
            f"/engines/{engine_revision_key.engine_id}"
            f"/engineRevisions/{engine_revision_key.engine_revision_id}",
        )
        engine_spec: dict = response.json()["engine_revision"]
        return EngineRevision.parse_obj(engine_spec)

    def create_analytics_engine(
        self,
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
        engine = self._default(
            name=name,
            settings=EngineSettings.analytics_default(),
            description=description,
            region_name=region_name,
        )
        return self.create_engine(
            engine=engine,
            engine_revision=EngineRevision.analytics_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            ),
        )

    def create_general_purpose_engine(
        self,
        name: str,
        description: Optional[str] = None,
        region_name: Optional[str] = None,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> Engine:
        """
        Create a new engine on Firebolt, based on default General Purpose settings.

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
        engine = self._default(
            name=name,
            settings=EngineSettings.general_purpose_default(),
            description=description,
            region_name=region_name,
        )
        return self.create_engine(
            engine=engine,
            engine_revision=EngineRevision.general_purpose_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            ),
        )

    @staticmethod
    def _default(
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

    def get_engines_by_ids(self, engine_ids: list[str]) -> list[Engine]:
        """Get multiple Engines from Firebolt by their ids."""
        response = self.firebolt_client.post(
            url=f"/core/v1/engines:getByIds",
            json={
                "engine_ids": [
                    {"account_id": self.account_id, "engine_id": engine_id}
                    for engine_id in engine_ids
                ]
            },
        )
        return [Engine.parse_obj(e) for e in response.json()["engines"]]

    def bind_engine_to_database(
        self, engine: Engine, database: Database, is_default_engine: bool
    ) -> Binding:
        """
        Attach this engine to a database.

        Args:
            engine: Engine to attach to the database.
            database: Database to which the engine will be attached.
            is_default_engine:
                Whether this engine should be used as default for this database.
                Only one engine can be set as default for a single database.
                This will overwrite any existing default.
        """
        return BindingService(self.firebolt_client).create_binding(
            engine=engine, database=database, is_default_engine=is_default_engine
        )

    def create_engine(
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

        response = self.firebolt_client.post(
            url="/core/v1/account/engines",
            headers={"Content-type": "application/json"},
            json=_EngineCreateRequest(
                account_id=self.account_id,
                engine=engine,
                engine_revision=engine_revision,
            ).dict(by_alias=True),
        )
        return Engine.parse_obj(response.json()["engine"])

    def start_engine(
        self,
        engine: Engine,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        print_dots: bool = True,
    ) -> Engine:
        """
        Start an engine. If it's already started, do nothing.

        Args:
            engine:
                The engine to start.
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
        if engine.engine_id is None:
            raise ValueError("engine_id must be set before starting")
        response = self.firebolt_client.post(
            url=f"/core/v1/account/engines/{engine.engine_id}:start",
        )
        engine = Engine.parse_obj(response.json()["engine"])
        status = engine.current_status_summary
        logger.info(
            f"Starting Engine engine_id={engine.engine_id} "
            f"name={engine.name} status_summary={status}"
        )
        start_time = time.time()
        end_time = start_time + wait_timeout_seconds

        # summary statuses: https://tinyurl.com/as7a9ru9
        while wait_for_startup and status != "ENGINE_STATUS_SUMMARY_RUNNING":
            if time.time() >= end_time:
                raise TimeoutError(
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                )
            engine = engine.get_by_id(engine_id=engine.engine_id)
            new_status = engine.current_status_summary
            if new_status != status:
                logger.info(f"Engine status_summary={new_status}")
            elif print_dots:
                print(".", end="")
            time.sleep(5)
            status = new_status
        return engine

    def stop_engine(self, engine: Engine) -> Engine:
        """Stop an Engine running on Firebolt."""
        response = self.firebolt_client.post(
            url=f"/core/v1/account/engines/{engine.engine_id}:stop",
        )
        return Engine.parse_obj(response.json()["engine"])

    def delete(self, engine: Engine) -> Engine:
        """Delete an Engine from Firebolt."""
        response = self.firebolt_client.delete(
            url=f"/core/v1"
            f"/accounts/{self.account_id}"
            f"/engines/{engine.engine_id}",
        )
        return Engine.parse_obj(response.json()["engine"])
