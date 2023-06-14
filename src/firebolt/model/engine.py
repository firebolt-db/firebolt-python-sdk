from __future__ import annotations

import functools
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Tuple

from firebolt.db import Connection, connect
from firebolt.model import FireboltBaseModel
from firebolt.model.database import Database
from firebolt.service.types import EngineStatus, WarmupMethod
from firebolt.utils.exception import DatabaseNotFoundError

if TYPE_CHECKING:
    from firebolt.service.engine import EngineService

logger = logging.getLogger(__name__)


def check_attached_to_database(func: Callable) -> Callable:
    """(Decorator) Ensure the engine is attached to a database."""

    @functools.wraps(func)
    def inner(self: Engine, *args: Any, **kwargs: Any) -> Any:
        # if self.database is None:
        #     raise NoAttachedDatabaseError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


@dataclass
class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, ingestion).

    Engines are configured in :py:class:`Settings
    <firebolt.model.engine.EngineSettings>`
    and in :py:class:`EngineRevisionSpecification
    <firebolt.model.engine_revision.EngineRevisionSpecification>`.
    """

    START_SQL: ClassVar[str] = "START ENGINE {}"
    STOP_SQL: ClassVar[str] = "STOP ENGINE {}"
    ALTER_PREFIX_SQL: ClassVar[str] = "ALTER ENGINE {} SET "
    ALTER_PARAMETER_NAMES: ClassVar[Tuple] = (
        "SCALE",
        "SPEC",
        "AUTO_STOP",
        "RENAME_TO",
        "WARMUP",
    )
    DROP_SQL: ClassVar[str] = "DROP ENGINE {}"

    _service: EngineService = field()

    name: str = field(metadata={"db_name": "engine_name"})
    region: str = field()
    spec: str = field()
    scale: int = field()
    current_status: str = field(metadata={"db_name": "status"})
    _database_name: Optional[str] = field(metadata={"db_name": "attached_to"})
    version: str = field()
    endpoint: str = field(metadata={"db_name": "url"})
    warmup: str = field()
    auto_stop: int = field()
    type: str = field()
    provisioning: str = field()

    @property
    def database(self) -> Optional[Database]:
        if self._database_name:
            try:
                return self._service.resource_manager.databases.get(self._database_name)
            except DatabaseNotFoundError:
                pass
        return None

    def refresh(self) -> None:
        """Update attributes of the instance from Firebolt."""
        for name, value in self._service._get_dict(self.name).items():
            setattr(self, name, value)

    def attach_to_database(self, database_name: str) -> None:
        """
        Attach this engine to a database.

        Args:
            database: Database to which the engine will be attached
        """
        return self._service.attach_to_database(self.name, database_name)

    @check_attached_to_database
    def get_connection(self) -> Connection:
        """Get a connection to the attached database for running queries.

        Returns:
            firebolt.db.connection.Connection: engine connection instance

        """
        return connect(
            database=self._database_name,  # type: ignore # already checked by decorator
            # we always have firebolt Auth as a client auth
            auth=self._service.client.auth,  # type: ignore
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
                f"{self.current_status.lower()}, waiting"
            )
            time.sleep(interval_seconds)
            if time.time() > timeout_time:
                raise TimeoutError(
                    f"Excedeed timeout of {wait_timeout}s waiting for "
                    f"an engine in {self.current_status.lower()} state"
                )
            logger.info(".[!n]")
            self.refresh()

    @check_attached_to_database
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
                f"in {self.current_status.lower()} state"
            )

        logger.info(f"Starting engine {self.name}")
        with self._service._connection.cursor() as c:
            c.execute(self.START_SQL.format(self.name))
        self.refresh()
        return self

    @check_attached_to_database
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
                f"in {self.current_status.lower()} state"
            )
        logger.info(f"Stopping engine {self.name}")
        with self._service._connection.cursor() as c:
            c.execute(self.STOP_SQL.format(self.name))
        self.refresh()
        return self

    def update(
        self,
        name: Optional[str] = None,
        scale: Optional[int] = None,
        spec: Optional[str] = None,
        auto_stop: Optional[int] = None,
        warmup: Optional[WarmupMethod] = None,
    ) -> Engine:
        """
        Updates the engine and returns an updated version of the engine. If all
        parameters are set to None, old engine parameter values remain.
        """

        if not any((name, scale, spec, auto_stop, warmup)):
            # Nothing to be updated
            return self

        self._wait_for_start_stop()
        if self.current_status in (EngineStatus.DROPPING, EngineStatus.REPAIRING):
            raise ValueError(
                f"Unable to update engine {self.name} because it's "
                f"in {self.current_status.lower()} state"
            )

        sql = self.ALTER_PREFIX_SQL.format(self.name)
        parameters = []
        for name, value in zip(
            self.ALTER_PARAMETER_NAMES, (scale, spec, auto_stop, name, warmup)
        ):
            if value:
                sql += f"{name} = ? "
                parameters.append(value)

        with self._service._connection.cursor() as c:
            c.execute(sql, parameters)
        self.refresh()
        return self

    @check_attached_to_database
    def restart(self) -> Engine:
        """
        Restart an engine.

        Returns:
            The updated engine from Firebolt.
        """
        self.stop()
        self.start()
        return self

    def delete(self) -> None:
        """Delete an engine."""
        self.refresh()
        if self.current_status == EngineStatus.DROPPING:
            return
        with self._service._connection.cursor() as c:
            c.execute(self.DROP_SQL.format(self.name))
