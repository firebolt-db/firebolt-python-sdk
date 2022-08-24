from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

from pydantic import Field, PrivateAttr

from firebolt.model import FireboltBaseModel
from firebolt.model.region import RegionKey
from firebolt.service.types import EngineStatusSummary
from firebolt.utils.exception import AttachedEngineInUseError
from firebolt.utils.urls import ACCOUNT_DATABASE_URL

if TYPE_CHECKING:
    from firebolt.model.binding import Binding
    from firebolt.model.engine import Engine
    from firebolt.service.database import DatabaseService

logger = logging.getLogger(__name__)


class DatabaseKey(FireboltBaseModel):
    account_id: str
    database_id: str


class FieldMask(FireboltBaseModel):
    paths: Sequence[str] = Field(alias="paths")


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

        logger.info(
            f"Deleting Database (database_id={self.database_id}, name={self.name})"
        )
        response = self._service.client.delete(
            url=ACCOUNT_DATABASE_URL.format(
                account_id=self._service.account_id, database_id=self.database_id
            ),
            headers={"Content-type": "application/json"},
        )
        return Database.parse_obj_with_service(
            response.json()["database"], self._service
        )

    def update(self, description: str) -> Database:
        """
        Updates a database description.
        """

        class _DatabaseUpdateRequest(FireboltBaseModel):
            """Helper model for sending Database creation requests."""

            account_id: str
            database: Database
            database_id: str
            update_mask: FieldMask

        self.description = description

        logger.info(
            f"Updating Database (database_id={self.database_id}, "
            f"name={self.name}, description={self.description})"
        )

        payload = _DatabaseUpdateRequest(
            account_id=self._service.account_id,
            database=self,
            database_id=self.database_id,
            update_mask=FieldMask(paths=["description"]),
        ).jsonable_dict(by_alias=True)

        response = self._service.client.patch(
            url=ACCOUNT_DATABASE_URL.format(
                account_id=self._service.account_id, database_id=self.database_id
            ),
            headers={"Content-type": "application/json"},
            json=payload,
        )

        return Database.parse_obj_with_service(
            response.json()["database"], self._service
        )

    def get_default_engine(self) -> Optional[Engine]:
        """
        Returns: default engine of the database, or None if default engine is missing
        """
        rm = self._service.resource_manager
        default_engines = [
            rm.engines.get(binding.engine_id)
            for binding in rm.bindings.get_many(database_id=self.database_id)
            if binding.is_default_engine
        ]

        return None if len(default_engines) == 0 else default_engines[0]
