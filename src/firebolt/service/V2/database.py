import logging
from typing import List, Optional, Union

from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine
from firebolt.service.V2.base import BaseService
from firebolt.utils.exception import DatabaseNotFoundError

logger = logging.getLogger(__name__)


class DatabaseService(BaseService):
    DB_FIELDS = (
        "database_name",
        "description",
        "region",
        "uncompressed_size",
        "compressed_size",
        "attached_engines",
        "created_on",
        "created_by",
        "errors",
    )
    GET_SQL = f"SELECT {', '.join(DB_FIELDS)} FROM information_schema.databases"
    GET_BY_NAME_SQL = GET_SQL + " WHERE database_name=?"
    GET_WHERE_SQL = " WHERE "

    CREATE_PREFIX_SQL = "CREATE DATABASE {}{}"
    CREATE_WITH_SQL = " WITH "
    IF_NOT_EXISTS_SQL = "IF NOT EXISTS "
    CREATE_PARAMETER_NAMES = (
        "REGION",
        "ATTACHED_ENGINES",
        "DESCRIPTION",
    )

    def _get_dict(self, name: str) -> dict:
        with self._connection.cursor() as c:
            count = c.execute(self.GET_BY_NAME_SQL, (name,))
            if count == 0:
                raise DatabaseNotFoundError(name)
            return {
                column.name: value for column, value in zip(c.description, c.fetchone())
            }

    def get(self, name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        return Database._from_dict(self._get_dict(name), self)

    def get_by_name(self, name: str) -> Database:
        return self.get(name)

    def get_many(
        self,
        name_contains: Optional[str] = None,
        attached_engine_name_eq: Optional[str] = None,
        attached_engine_name_contains: Optional[str] = None,
        region_eq: Optional[str] = None,
    ) -> List[Database]:
        """
        Get a list of databases on Firebolt.

        Args:
            name_contains: Filter for databases with a name containing this substring
            attached_engine_name_eq: Filter for databases by an exact engine name
            attached_engine_name_contains: Filter for databases by engines with a
                name containing this substring
            region_eq: Filter for database by region

        Returns:
            A list of databases matching the filters
        """
        sql = self.GET_SQL
        parameters = []
        if any(
            (
                name_contains,
                attached_engine_name_eq,
                attached_engine_name_contains,
                region_eq,
            )
        ):
            condition = []
            if name_contains:
                condition.append("database_name like ?")
                parameters.append(f"%{name_contains}%")
            if attached_engine_name_eq:
                condition.append(
                    "any_match(eng -> split_part(eng, ' ', 1) = ?,"
                    " split(',', attached_engines))"
                )
                parameters.append(attached_engine_name_eq)
            if attached_engine_name_contains:
                condition.append(
                    "any_match(eng -> split_part(eng, ' ', 1) like ?,"
                    " split(',', attached_engines))"
                )
                parameters.append(f"%{attached_engine_name_contains}%")
            if region_eq:
                condition.append("region = ?")
                parameters.append(str(region_eq))
            sql += self.GET_WHERE_SQL + " AND ".join(condition)

        with self._connection.cursor() as c:
            c.execute(sql, parameters)
            dicts = [
                {column.name: value for column, value in zip(c.description, row)}
                for row in c.fetchall()
            ]
            return [Database._from_dict(_dict, self) for _dict in dicts]

    def create(
        self,
        name: str,
        region: Optional[str] = None,
        attached_engines: Union[List[str], List[Engine], None] = None,
        description: Optional[str] = None,
        fail_if_exists: bool = True,
    ) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            name: Name of the database
            attached_engines: List of engines to attach to the database
            region: Region name in which to create the database
            fail_if_exists: Fail is a database with provided name already exists

        Returns:
            The newly created database
        """

        logger.info(f"Creating database {name}")

        sql = self.CREATE_PREFIX_SQL.format(
            ("" if fail_if_exists else self.IF_NOT_EXISTS_SQL), name
        )
        parameters = []
        if any((region, attached_engines, description)):
            sql += self.CREATE_WITH_SQL
            for param, value in zip(
                self.CREATE_PARAMETER_NAMES,
                (region, attached_engines, description),
            ):
                if value:
                    sql += f"{param} = ? "
                    # Convert list of engines to a list of their names
                    if (
                        isinstance(value, list)
                        and len(value) > 0
                        and isinstance(value[0], Engine)
                    ):
                        value = [eng.name for eng in value]  # type: ignore
                    parameters.append(value)
        with self._connection.cursor() as c:
            c.execute(sql, parameters)
        return self.get(name)
