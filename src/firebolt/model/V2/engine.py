from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, List, Optional, Tuple, Union

from firebolt.db import Connection, connect
from firebolt.model.V2 import FireboltBaseModel
from firebolt.model.V2.database import Database
from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.V2.types import EngineStatus
from firebolt.utils.exception import DatabaseNotFoundError

if TYPE_CHECKING:
    from firebolt.service.V2.engine import EngineService

logger = logging.getLogger(__name__)


@dataclass
class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, ingestion).
    """

    START_SQL: ClassVar[str] = 'START ENGINE "{}"'
    STOP_SQL: ClassVar[str] = 'STOP ENGINE "{}"'
    ALTER_PREFIX_SQL: ClassVar[str] = 'ALTER ENGINE "{}" '
    ALTER_PARAMETER_NAMES: ClassVar[Tuple] = (
        "NODES",
        "TYPE",
        "AUTO_STOP",
    )
    DROP_SQL: ClassVar[str] = 'DROP ENGINE "{}"'

    # Engine names can only contain alphanumeric characters and underscores
    _engine_name_re = re.compile(r"^[a-zA-Z0-9_]+$")

    _service: EngineService = field(repr=False, compare=False)

    name: str = field(metadata={"db_name": "engine_name"})
    region: str = field()
    spec: InstanceType = field(metadata={"db_name": "type"})
    scale: int = field(metadata={"db_name": "nodes"})
    current_status: EngineStatus = field(metadata={"db_name": "status"})
    _database_name: str = field(
        repr=False, metadata={"db_name": ("attached_to", "default_database")}
    )
    version: str = field()
    endpoint: str = field(metadata={"db_name": "url"})
    warmup: str = field()
    auto_stop: int = field()
    type: str = field(metadata={"db_name": "engine_type"})

    def __post_init__(self) -> None:
        # Specs are just strings for accounts v2
        if isinstance(self.spec, str) and self.spec:
            # Resolve engine specification
            self.spec = InstanceType(self.spec)
        if isinstance(self.current_status, str) and self.current_status:
            # Resolve engine status
            self.current_status = EngineStatus(self.current_status)

    @property
    def database(self) -> Optional[Database]:
        if self._database_name:
            try:
                return self._service.resource_manager.databases.get(self._database_name)
            except DatabaseNotFoundError:
                pass
        return None

    def refresh(self, name: Optional[str] = None) -> None:
        """Update attributes of the instance from Firebolt."""
        field_name_overrides = self._get_field_overrides()
        for field_name, value in self._service._get_dict(name or self.name).items():
            setattr(self, field_name_overrides.get(field_name, field_name), value)

        self.__post_init__()

    def attach_to_database(self, database: Union[Database, str]) -> None:
        """
        Attach this engine to a database.

        Args:
            database: Database to which the engine will be attached
        """
        self._service.attach_to_database(self, database)

    def get_connection(self) -> Connection:
        """Get a connection to the attached database for running queries.

        Returns:
            firebolt.db.connection.Connection: engine connection instance

        """
        return connect(
            database=self._database_name,  # type: ignore # already checked by decorator
            # we always have firebolt Auth as a client auth
            auth=self._service._connection._client.auth,  # type: ignore
            engine_name=self.name,
            account_name=self._service.resource_manager.account_name,
            api_endpoint=self._service.resource_manager.api_endpoint,
        )

    def _wait_for_start_stop(self) -> None:
        wait_timeout = 3600
        interval_seconds = 5
        timeout_time = time.time() + wait_timeout
        while self.current_status in (EngineStatus.STOPPING, EngineStatus.STARTING):
            logger.info(
                f"Engine {self.name} is currently "
                f"{self.current_status.value.lower()}, waiting"
            )
            time.sleep(interval_seconds)
            if time.time() > timeout_time:
                raise TimeoutError(
                    f"Excedeed timeout of {wait_timeout}s waiting for "
                    f"an engine in {self.current_status.value.lower()} state"
                )
            logger.info(".[!n]")
            self.refresh()

    def start(self) -> Engine:
        """
        Start an engine. If it's already started, do nothing.

        Returns:
            The updated engine instance.
        """

        self.refresh()
        self._wait_for_start_stop()
        if self.current_status == EngineStatus.RUNNING:
            logger.info(f"Engine {self.name} is already running.")
            return self
        if self.current_status in (EngineStatus.DROPPING, EngineStatus.REPAIRING):
            raise ValueError(
                f"Unable to start engine {self.name} because it's "
                f"in {self.current_status.value.lower()} state"
            )

        logger.info(f"Starting engine {self.name}")
        with self._service._connection.cursor() as c:
            c.execute(self.START_SQL.format(self.name))
        self.refresh()
        return self

    def stop(self) -> Engine:
        """Stop an engine. If it's already stopped, do nothing.

        Returns:
            The updated engine instance.
        """
        self.refresh()
        self._wait_for_start_stop()
        if self.current_status == EngineStatus.STOPPED:
            logger.info(f"Engine {self.name} is already stopped.")
            return self
        if self.current_status in (EngineStatus.DROPPING, EngineStatus.REPAIRING):
            raise ValueError(
                f"Unable to stop engine {self.name} because it's "
                f"in {self.current_status.value.lower()} state"
            )
        logger.info(f"Stopping engine {self.name}")
        with self._service._connection.cursor() as c:
            c.execute(self.STOP_SQL.format(self.name))
        self.refresh()
        return self

    def update(
        self,
        name: Optional[str] = None,
        engine_type: Optional[str] = None,
        scale: Optional[int] = None,
        spec: Union[InstanceType, str, None] = None,
        auto_stop: Optional[int] = None,
        warmup: Optional[str] = None,
    ) -> Engine:
        """
        Updates the engine and returns an updated version of the engine. If all
        parameters are set to None, old engine parameter values remain.
        """
        disallowed = [
            name
            for name, value in (("engine_type", engine_type), ("warmup", warmup))
            if value
        ]
        if disallowed:
            raise ValueError(
                f"Parameters {disallowed} are not supported for this account"
            )

        if not any(x is not None for x in (name, scale, spec, auto_stop)):
            # Nothing to be updated
            return self

        if name is not None and any(x is not None for x in (scale, spec, auto_stop)):
            raise ValueError("Cannot update name and other parameters at the same time")

        self.refresh()
        self._wait_for_start_stop()
        if self.current_status in (EngineStatus.DROPPING, EngineStatus.REPAIRING):
            raise ValueError(
                f"Unable to update engine {self.name} because it's "
                f"in {self.current_status.value.lower()} state"
            )

        sql = self.ALTER_PREFIX_SQL.format(self.name)
        parameters: List[Union[str, int]] = []
        if name is not None:
            if not self._engine_name_re.match(name):
                raise ValueError(
                    f"Engine name {name} is invalid, "
                    "it must only contain alphanumeric characters and underscores."
                )
            sql += f" RENAME TO {name}"
        else:
            sql += " SET "
            parameters = []
            for param, value in zip(
                self.ALTER_PARAMETER_NAMES,
                (scale, spec, auto_stop),
            ):
                if value is not None:
                    sql_part, new_params = self._service._format_engine_attribute_sql(
                        param, value
                    )
                    sql += sql_part
                    parameters.extend(new_params)

        with self._service._connection.cursor() as c:
            c.execute(sql, parameters)
        self.refresh(name)
        return self

    def delete(self) -> None:
        """Delete an engine."""
        self.refresh()
        if self.current_status in [EngineStatus.DROPPING, EngineStatus.DELETING]:
            return
        with self._service._connection.cursor() as c:
            c.execute(self.DROP_SQL.format(self.name))
