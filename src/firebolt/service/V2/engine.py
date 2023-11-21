from logging import getLogger
from typing import List, Optional, Union

from firebolt.model.V2.engine import Database, Engine
from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.V2.base import BaseService
from firebolt.service.V2.types import EngineStatus, EngineType, WarmupMethod
from firebolt.utils.exception import EngineNotFoundError

logger = getLogger(__name__)


class EngineService(BaseService):
    DB_FIELDS = (
        "engine_name",
        "region",
        "spec",
        "scale",
        "status",
        "attached_to",
        "version",
        "url",
        "warmup",
        "auto_stop",
        "engine_type",
    )
    GET_SQL = f"SELECT {', '.join(DB_FIELDS)} FROM information_schema.engines"
    GET_BY_NAME_SQL = GET_SQL + " WHERE engine_name=?"
    GET_WHERE_SQL = " WHERE "

    CREATE_PREFIX_SQL = "CREATE ENGINE {}{}"
    IF_NOT_EXISTS_SQL = "IF NOT EXISTS "
    CREATE_WITH_SQL = " WITH "
    CREATE_PARAMETER_NAMES = (
        "REGION",
        "ENGINE_TYPE",
        "SPEC",
        "SCALE",
        "AUTO_STOP",
        "WARMUP",
    )

    ATTACH_TO_DB_SQL = "ATTACH ENGINE {} TO {}"

    def _get_dict(self, name: str) -> dict:
        with self._connection.cursor() as c:
            count = c.execute(self.GET_BY_NAME_SQL, (name,))
            if count == 0:
                raise EngineNotFoundError(name)
            return {
                column.name: value for column, value in zip(c.description, c.fetchone())
            }

    def get(self, name: str) -> Engine:
        """Get an engine from Firebolt by its name."""
        return Engine._from_dict(self._get_dict(name), self)

    def get_by_name(self, name: str) -> Engine:
        return self.get(name)

    def get_many(
        self,
        name_contains: Optional[str] = None,
        current_status_eq: Union[str, EngineStatus, None] = None,
        current_status_not_eq: Union[str, EngineStatus, None] = None,
        region_eq: Optional[str] = None,
        database_name: Optional[str] = None,
    ) -> List[Engine]:
        """
        Get a list of engines on Firebolt.

        Args:
            name_contains: Filter for engines with a name containing this substring
            current_status_eq: Filter for engines with this status
            current_status_not_eq: Filter for engines that do not have this status
            region_eq: Filter for engines by region

        Returns:
            A list of engines matching the filters
        """
        sql = self.GET_SQL
        parameters = []
        if any(
            (
                name_contains,
                current_status_eq,
                current_status_not_eq,
                region_eq,
                database_name,
            )
        ):
            condition = []
            if name_contains:
                condition.append("engine_name like ?")
                parameters.append(f"%{name_contains}%")
            if current_status_eq:
                condition.append("status = ?")
                parameters.append(str(current_status_eq))
            if current_status_not_eq:
                condition.append("status != ?")
                parameters.append(str(current_status_eq))
            if region_eq:
                condition.append("region = ?")
                parameters.append(region_eq)
            if database_name:
                condition.append("attached_to = ?")
                parameters.append(database_name)
            sql += self.GET_WHERE_SQL + " AND ".join(condition)

        with self._connection.cursor() as c:
            c.execute(sql, parameters)
            dicts = [
                {column.name: value for column, value in zip(c.description, row)}
                for row in c.fetchall()
            ]
            return [Engine._from_dict(_dict, self) for _dict in dicts]

    def create(
        self,
        name: str,
        region: Optional[str] = None,
        engine_type: Union[str, EngineType] = EngineType.GENERAL_PURPOSE,
        spec: Union[InstanceType, str, None] = None,
        scale: Optional[int] = None,
        auto_stop: Optional[int] = None,
        warmup: Union[str, WarmupMethod, None] = None,
        fail_if_exists: bool = True,
    ) -> Engine:
        """
        Create a new engine.

        Args:
            name: An identifier that specifies the name of the engine
            region: The AWS region in which the engine runs
            engine_type: The engine type. GENERAL_PURPOSE or DATA_ANALYTICS
            spec: Firebolt instance type. If not set, will default to
                the cheapest instance.
            scale: The number of compute instances on the engine.
                The scale can be any int from 1 to 128.
            auto_stop: The amount of time (in minutes)
            after which the engine automatically stops
            warmup: The warmup method that should be used:

                `MINIMAL` - On-demand loading (both indexes and tables' data)

                `PRELOAD_INDEXES` - Load indexes only

                `PRELOAD_ALL_DATA` - Full data auto-load
                (both indexes and table data - full warmup)
            fail_if_exists: Fail is an engine with provided name already exists

        Returns:
            Engine with the specified settings
        """

        logger.info(f"Creating engine {name}")

        sql = self.CREATE_PREFIX_SQL.format(
            ("" if fail_if_exists else self.IF_NOT_EXISTS_SQL), name
        )
        parameters = []
        if any(
            x is not None for x in (region, engine_type, spec, scale, auto_stop, warmup)
        ):
            sql += self.CREATE_WITH_SQL
            for param, value in zip(
                self.CREATE_PARAMETER_NAMES,
                (region, engine_type, spec, scale, auto_stop, warmup),
            ):
                if value is not None:
                    sql += f"{param} = ? "
                    parameters.append(str(value))
        with self._connection.cursor() as c:
            c.execute(sql, parameters)
        return self.get(name)

    def attach_to_database(
        self, engine: Union[Engine, str], database: Union[Database, str]
    ) -> None:
        engine_name = engine.name if isinstance(engine, Engine) else engine
        database_name = database.name if isinstance(database, Database) else database
        with self._connection.cursor() as c:
            c.execute(self.ATTACH_TO_DB_SQL.format(engine_name, database_name))
        if isinstance(engine, Engine):
            engine._database_name = (
                database.name if isinstance(database, Database) else database
            )
