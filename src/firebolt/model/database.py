from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Optional

from pydantic import Field, PrivateAttr

from firebolt.common.exception import AttachedEngineInUseError
from firebolt.model import FireboltBaseModel
from firebolt.model.region import RegionKey
from firebolt.service.types import EngineStatusSummary

if TYPE_CHECKING:
    from firebolt.model.binding import Binding
    from firebolt.model.engine import Engine
    from firebolt.service.database import DatabaseService


class DatabaseKey(FireboltBaseModel):
    account_id: str
    database_id: str


class Database(FireboltBaseModel):
    """
    A Firebolt database.

    Databases belong to a region and have a description,
    but otherwise are not configurable.
    """

    # internal
    _service: DatabaseService = PrivateAttr()

    # required
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    compute_region_key: RegionKey = Field(alias="compute_region_id")

    # optional
    database_key: Optional[DatabaseKey] = Field(alias="id")
    description: Optional[str] = Field(max_length=255)
    emoji: Optional[str] = Field(max_length=255)
    current_status: Optional[str]
    health_status: Optional[str]
    data_size_full: Optional[int]
    data_size_compressed: Optional[int]
    is_system_database: Optional[bool]
    storage_bucket_name: Optional[str]
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    desired_status: Optional[str]

    @classmethod
    def parse_obj_with_service(
        cls, obj: Any, database_service: DatabaseService
    ) -> Database:
        database = cls.parse_obj(obj)
        database._service = database_service
        return database

    @property
    def database_id(self) -> Optional[str]:
        if self.database_key is None:
            return None
        return self.database_key.database_id

    def get_attached_engines(self) -> List[Engine]:
        """Get a list of engines that are attached to this database."""
        return self._service.resource_manager.bindings.get_engines_bound_to_database(  # noqa: E501
            database=self
        )

    def attach_to_engine(
        self, engine: Engine, is_default_engine: bool = False
    ) -> Binding:
        """
        Attach an engine to this database.

        Args:
            engine: The engine to attach.
            is_default_engine:
                Whether this engine should be used as default for this database.
                Only one engine can be set as default for a single database.
                This will overwrite any existing default.
        """
        return self._service.resource_manager.bindings.create(
            engine=engine, database=self, is_default_engine=is_default_engine
        )

    def delete(self) -> Database:
        """
        Delete a database from Firebolt.

        Raises an error if there are any attached engines.
        """
        for engine in self.get_attached_engines():
            if engine.current_status_summary in {
                EngineStatusSummary.ENGINE_STATUS_SUMMARY_STARTING,
                EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
            }:
                raise AttachedEngineInUseError(method_name="delete")

        response = self._service.client.delete(
            url=f"/core/v1/account/databases/{self.database_id}",
            headers={"Content-type": "application/json"},
        )
        return Database.parse_obj_with_service(
            response.json()["database"], self._service
        )
