from logging import getLogger
from typing import List, Optional, Union

from firebolt.model.engine import Engine
from firebolt.model.region import Region
from firebolt.service.base import BaseService
from firebolt.service.types import EngineStatus, EngineType, WarmupMethod
from firebolt.utils.exception import EngineNotFoundError

logger = getLogger(__name__)


class EngineService(BaseService):
    ENGINE_DB_FIELDS = (
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
        "type",
        "provisioning",
    )
    GET_SQL = f"SELECT {', '.join(ENGINE_DB_FIELDS)} FROM information_schema.engines"
    GET_BY_NAME_SQL = GET_SQL + " WHERE engine_name=?"
    GET_WHERE_SQL = " WHERE "

    CREATE_PREFIX_SQL = "CREATE ENGINE {}"
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
        engine_dict = self._get_dict(name)
        return Engine._from_dict(engine_dict, self)

    def get_many(
        self,
        name_contains: Optional[str] = None,
        current_status_eq: Union[str, EngineStatus, None] = None,
        current_status_not_eq: Union[str, EngineStatus, None] = None,
        region_eq: Union[str, Region, None] = None,
    ) -> List[Engine]:
        """
        Get a list of engines on Firebolt.

        Args:
            name_contains: Filter for engines with a name containing this substring
            current_status_eq: Filter for engines with this status
            current_status_not_eq: Filter for engines that do not have this status
            region_eq: Filter for engines by region
            order_by: Method by which to order the results. See [EngineOrder]

        Returns:
            A list of engines matching the filters
        """
        sql = self.GET_SQL
        parameters = []
        if any((name_contains, current_status_eq, current_status_not_eq, region_eq)):
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
                parameters.append(str(region_eq))
            sql += self.GET_WHERE_SQL + " AND ".join(condition)

        with self._connection.cursor() as c:
            c.execute(sql, parameters)
            engine_dicts = [
                {column.name: value for column, value in zip(c.description, engine_row)}
                for engine_row in c.fetchall()
            ]
            return [
                Engine._from_dict(engine_dict, self) for engine_dict in engine_dicts
            ]

    def create(
        self,
        name: str,
        region: Union[str, Region, None] = None,
        engine_type: Union[str, EngineType] = EngineType.GENERAL_PURPOSE,
        spec: Optional[str] = None,
        scale: Optional[int] = None,
        auto_stop: Optional[int] = None,
        warmup: Union[str, WarmupMethod, None] = None,
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

        Returns:
            Engine with the specified settings
        """

        logger.info(f"Creating engine {name}")

        sql = self.CREATE_PREFIX_SQL.format(name)
        parameters = []
        if any((region, engine_type, spec, scale, auto_stop, warmup)):
            sql += self.CREATE_WITH_SQL
            for name, value in zip(
                self.CREATE_PARAMETER_NAMES,
                (region, engine_type, spec, scale, auto_stop, warmup),
            ):
                if value:
                    sql += f"{name} = ? "
                    parameters.append(value)
        with self._connection.cursor() as c:
            c.execute(sql, parameters)
        return self.get(name)

    def attach_to_database(self, engine_name: str, database_name: str) -> None:
        with self._connection.cursor() as c:
            c.execute(self.ATTACH_TO_DB_SQL.format(engine_name, database_name))
