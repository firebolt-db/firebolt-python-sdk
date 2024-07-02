from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, List

from firebolt.model.V2 import FireboltBaseModel
from firebolt.service.V2.types import EngineStatus
from firebolt.utils.exception import AttachedEngineInUseError

if TYPE_CHECKING:
    from firebolt.model.V2.engine import Engine
    from firebolt.service.V2.database import DatabaseService

logger = logging.getLogger(__name__)


@dataclass
class Database(FireboltBaseModel):
    """
    A Firebolt database.

    Databases belong to a region and have a description,
    but otherwise are not configurable.
    """

    ALTER_SQL: ClassVar[str] = 'ALTER DATABASE "{}" SET DESCRIPTION = ?'

    DROP_SQL: ClassVar[str] = 'DROP DATABASE "{}"'

    # internal
    _service: DatabaseService = field(repr=False, compare=False)

    # required
    name: str = field(metadata={"db_name": "catalog_name"})
    description: str = field()
    create_time: datetime = field(metadata={"db_name": "created"})
    create_actor: str = field(metadata={"db_name": "catalog_owner"})

    def get_attached_engines(self) -> List[Engine]:
        """Get a list of engines that are attached to this database."""
        return self._service.resource_manager.engines.get_many(database_name=self.name)

    def attach_engine(self, engine: Engine) -> None:
        """
        Attach an engine to this database.

        Args:
            engine: The engine to attach.
        """
        return self._service.resource_manager.engines.attach_to_database(
            engine.name, self.name
        )

    def update(self, description: str) -> Database:
        """
        Updates a database description.
        """
        if not description:
            return self

        for engine in self.get_attached_engines():
            if engine.current_status not in {
                EngineStatus.RUNNING,
                EngineStatus.STOPPED,
            }:
                raise AttachedEngineInUseError(method_name="update")

        sql = self.ALTER_SQL.format(self.name)
        with self._service._connection.cursor() as c:
            c.execute(sql, (description,))
        self.description = description
        return self

    def delete(self) -> None:
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

        with self._service._connection.cursor() as c:
            c.execute(self.DROP_SQL.format(self.name))
