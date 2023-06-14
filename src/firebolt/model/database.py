from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Sequence

from pydantic import Field

from firebolt.model import FireboltBaseModel
from firebolt.model.region import RegionKey
from firebolt.service.types import EngineStatus
from firebolt.utils.exception import AttachedEngineInUseError
from firebolt.utils.urls import ACCOUNT_DATABASE_URL

if TYPE_CHECKING:
    from firebolt.model.binding import Binding
    from firebolt.model.engine import Engine
    from firebolt.service.database import DatabaseService

logger = logging.getLogger(__name__)


@dataclass
class DatabaseKey:
    account_id: str
    database_id: str


@dataclass
class FieldMask:
    paths: Sequence[str] = Field(alias="paths")


@dataclass
class Database(FireboltBaseModel):
    """
    A Firebolt database.

    Databases belong to a region and have a description,
    but otherwise are not configurable.
    """

    # internal
    _service: DatabaseService = field()

    # required
    name: str = field(metadata={"db_name": "database_name"})
    description: str = field()
    compute_region_key: RegionKey = field()

    # optional
    database_key: Optional[DatabaseKey] = field(default=None)
    emoji: Optional[str] = field(default=None)
    current_status: Optional[str] = field(default=None)
    health_status: Optional[str] = field(default=None)
    data_size_full: Optional[int] = field(default=None)
    data_size_compressed: Optional[int] = field(default=None)
    is_system_database: Optional[bool] = field(default=None)
    storage_bucket_name: Optional[str] = field(default=None)
    create_time: Optional[datetime] = field(default=None)
    create_actor: Optional[str] = field(default=None)
    last_update_time: Optional[datetime] = field(default=None)
    last_update_actor: Optional[str] = field(default=None)
    desired_status: Optional[str] = field(default=None)

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
            if engine.current_status in {
                EngineStatus.STARTING,
                EngineStatus.STOPPING,
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
        return Database._from_dict(response.json()["database"], self._service)

    def update(self, description: str) -> Database:
        """
        Updates a database description.
        """

        @dataclass
        class _DatabaseUpdateRequest:
            """Helper model for sending Database creation requests."""

            account_id: str = field()
            database: Database = field()
            database_id: Optional[str] = field()
            update_mask: FieldMask = field()

        self.description = description

        logger.info(
            f"Updating Database (database_id={self.database_id}, "
            f"name={self.name}, description={self.description})"
        )

        payload = asdict(
            _DatabaseUpdateRequest(
                account_id=self._service.account_id,
                database=self,
                database_id=self.database_id,
                update_mask=FieldMask(paths=["description"]),
            )
        )

        response = self._service.client.patch(
            url=ACCOUNT_DATABASE_URL.format(
                account_id=self._service.account_id, database_id=self.database_id
            ),
            headers={"Content-type": "application/json"},
            json=payload,
        )

        return Database._from_dict(response.json()["database"], self._service)

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
