import logging
from typing import List, Optional, Union

from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine
from firebolt.service.V2.base import BaseService
from firebolt.utils.exception import DatabaseNotFoundError

logger = logging.getLogger(__name__)


class DatabaseService(BaseService):
    DB_FIELDS = (
        "catalog_name",
        "description",
        "created",
        "catalog_owner",
    )
    GET_SQL = f"SELECT {', '.join(DB_FIELDS)} FROM information_schema.catalogs"
    GET_BY_NAME_SQL = GET_SQL + " WHERE catalog_name=?"
    GET_WHERE_SQL = " WHERE "

    CREATE_PREFIX_SQL = 'CREATE DATABASE {}"{}"'
    IF_NOT_EXISTS_SQL = "IF NOT EXISTS "

    DISALLOWED_ACCOUNT_V2_PARAMETERS = [
        "attached_engine_name_eq",
        "attached_engine_name_contains",
    ]

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
        disallowed_parameters = [
            name
            for name, value in (
                ("attached_engine_name_eq", attached_engine_name_eq),
                ("attached_engine_name_contains", attached_engine_name_contains),
                ("region_eq", region_eq),
            )
            if value
        ]
        if disallowed_parameters:
            raise ValueError(
                f"Parameters {disallowed_parameters} are not supported for this account"
            )

        if name_contains:
            sql += " WHERE catalog_name like ?"
            parameters.append(f"%{name_contains}%")

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
            region: Region name in which to create the database
            attached_engines: List of engines to attach to the database
            description: Description of the database
            fail_if_exists: Fail is a database with provided name already exists

        Returns:
            The newly created database
        """

        logger.info(f"Creating database {name}")

        disallowed_parameters = [
            name
            for name, value in (
                ("region", region),
                (attached_engines, attached_engines),
            )
            if value is not None
        ]
        if disallowed_parameters:
            raise ValueError(
                f"Parameters {disallowed_parameters} are not supported for this account"
            )

        sql = self.CREATE_PREFIX_SQL.format(
            ("" if fail_if_exists else self.IF_NOT_EXISTS_SQL), name
        )
        parameters = []
        if description:
            sql += " WITH DESCRIPTION = ? "
            parameters.append(description)

        with self._connection.cursor() as c:
            c.execute(sql, parameters)
        return self.get(name)
