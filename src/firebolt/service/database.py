from typing import List, Optional, Union

from firebolt.common.urls import (
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
)
from firebolt.model import FireboltBaseModel
from firebolt.model.database import Database
from firebolt.service.base import BaseService
from firebolt.service.types import DatabaseOrder


class DatabaseService(BaseService):
    def get(self, id_: str) -> Database:
        """Get a Database from Firebolt by its id."""
        response = self.client.get(
            url=ACCOUNT_DATABASE_URL.format(account_id=self.account_id, database_id=id_)
        )
        return Database.parse_obj_with_service(
            obj=response.json()["database"], database_service=self
        )

    def get_by_name(self, name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        database_id = self.get_id_by_name(name=name)
        return self.get(id_=database_id)

    def get_id_by_name(self, name: str) -> str:
        """Get a Database id from Firebolt by its name."""
        response = self.client.get(
            url=ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=self.account_id),
            params={"database_name": name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def get_many(
        self,
        name_contains: str,
        attached_engine_name_eq: str,
        attached_engine_name_contains: str,
        order_by: Union[str, DatabaseOrder],
    ) -> List[Database]:
        """
        Get a list of databases on Firebolt.

        Args:
            name_contains: Filter for databases with a name containing this substring.
            attached_engine_name_eq: Filter for databases by an exact engine name.
            attached_engine_name_contains: Filter for databases by engines with a
                name containing this substring.
            order_by: Method by which to order the results. See [DatabaseOrder].

        Returns:
            A list of databases matching the filters.
        """
        if isinstance(order_by, str):
            order_by = DatabaseOrder[order_by]
        response = self.client.get(
            url=ACCOUNT_DATABASES_URL.format(account_id=self.account_id),
            params={
                "filter.name_contains": name_contains,
                "filter.attached_engine_name_eq": attached_engine_name_eq,
                "filter.attached_engine_name_contains": attached_engine_name_contains,
                "order_by": order_by.name,
            },
        )
        return [
            Database.parse_obj_with_service(obj=d, database_service=self)
            for d in response.json()["databases"]
        ]

    def create(self, name: str, region: Optional[str] = None) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            name: Name of the database.
            region: Region name in which to create the database.

        Returns:
            The newly created Database.
        """

        class _DatabaseCreateRequest(FireboltBaseModel):
            """Helper model for sending Database creation requests."""

            account_id: str
            database: Database

        if region is None:
            region_key = self.resource_manager.regions.default_region.key
        else:
            region_key = self.resource_manager.regions.get_by_name(name=region).key
        database = Database(name=name, compute_region_key=region_key)

        response = self.client.post(
            url=ACCOUNT_DATABASES_URL.format(account_id=self.account_id),
            headers={"Content-type": "application/json"},
            json=_DatabaseCreateRequest(
                account_id=self.account_id,
                database=database,
            ).jsonable_dict(by_alias=True),
        )
        return Database.parse_obj_with_service(
            obj=response.json()["database"], database_service=self
        )
