from __future__ import annotations

import functools
import logging
import time
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Tuple,
    Union,
)

from firebolt.db import Connection, connect
from firebolt.model.V2 import FireboltBaseModel
from firebolt.model.V2.database import Database
from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.V2.types import EngineStatus, EngineType, WarmupMethod
from firebolt.utils.exception import (
    DatabaseNotFoundError,
    NoAttachedDatabaseError,
)

if TYPE_CHECKING:
    from firebolt.service.V2.engine import EngineService

logger = logging.getLogger(__name__)


def check_attached_to_database(func: Callable) -> Callable:
    """(Decorator) Ensure the engine is attached to a database."""

    @functools.wraps(func)
    def inner(self: Engine, *args: Any, **kwargs: Any) -> Any:
        if self.database is None:
            raise NoAttachedDatabaseError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


@dataclass
class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, ingestion).
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
        "ENGINE_TYPE",
    )
    DROP_SQL: ClassVar[str] = "DROP ENGINE {}"

    _service: EngineService = field(repr=False, compare=False)

    name: str = field(metadata={"db_name": "engine_name"})
    region: str = field()
    spec: InstanceType = field()
    scale: int = field()
    current_status: EngineStatus = field(metadata={"db_name": "status"})
    _database_name: str = field(repr=False, metadata={"db_name": "attached_to"})
    version: str = field()
    endpoint: str = field(metadata={"db_name": "url"})
    warmup: WarmupMethod = field()
    auto_stop: int = field()
    type: EngineType = field(metadata={"db_name": "engine_type"})

    def __post_init__(self) -> None:
        if isinstance(self.spec, str) and self.spec:
            # Resolve engine specification
            self.spec = self._service.resource_manager.instance_types.get(self.spec)
        if isinstance(self.current_status, str) and self.current_status:
            # Resolve engine status
            self.current_status = EngineStatus(self.current_status)
        if isinstance(self.warmup, str) and self.warmup:
            # Resolve warmup method
            self.warmup = WarmupMethod.from_display_name(self.warmup)
        if isinstance(self.type, str) and self.type:
            # Resolve engine type
            self.type = EngineType.from_display_name(self.type)

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
        field_name_overrides = self._get_field_overrides()
        for name, value in self._service._get_dict(self.name).items():
            setattr(self, field_name_overrides.get(name, name), value)

        self.__post_init__()

    def attach_to_database(self, database: Union[Database, str]) -> None:
        """
        Attach this engine to a database.

        Args:
            database: Database to which the engine will be attached
        """
        self._service.attach_to_database(self, database)

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
                f"in {self.current_status.value.lower()} state"
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
        engine_type: Union[EngineType, str, None] = None,
        scale: Optional[int] = None,
        spec: Union[InstanceType, str, None] = None,
        auto_stop: Optional[int] = None,
        warmup: Optional[WarmupMethod] = None,
    ) -> Engine:
        """
        Updates the engine and returns an updated version of the engine. If all
        parameters are set to None, old engine parameter values remain.
        """

        if not any(
            x is not None for x in (name, scale, spec, auto_stop, warmup, engine_type)
        ):
            # Nothing to be updated
            return self

        self.refresh()
        self._wait_for_start_stop()
        if self.current_status in (EngineStatus.DROPPING, EngineStatus.REPAIRING):
            raise ValueError(
                f"Unable to update engine {self.name} because it's "
                f"in {self.current_status.value.lower()} state"
            )

        sql = self.ALTER_PREFIX_SQL.format(self.name)
        parameters = []
        for param, value in zip(
            self.ALTER_PARAMETER_NAMES,
            (scale, spec, auto_stop, name, warmup, engine_type),
        ):
            if value is not None:
                sql += f"{param} = ? "
                parameters.append(str(value))

        with self._service._connection.cursor() as c:
            c.execute(sql, parameters)
        self.refresh()
        return self

    def delete(self) -> None:
        """Delete an engine."""
        self.refresh()
        if self.current_status == EngineStatus.DROPPING:
            return
        with self._service._connection.cursor() as c:
            c.execute(self.DROP_SQL.format(self.name))
