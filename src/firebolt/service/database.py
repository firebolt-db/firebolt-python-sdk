from typing import Union

from firebolt.model import FireboltBaseModel
from firebolt.model.database import Database
from firebolt.service.base import BaseService
from firebolt.service.types import DatabaseOrder


class DatabaseService(BaseService):
    def get(self, database_id: str) -> Database:
        """Get a Database from Firebolt by its id."""
        response = self.client.get(
            url=f"/core/v1/accounts/{self.account_id}/databases/{database_id}",
        )
        return Database.parse_obj_with_service(
            obj=response.json()["database"], database_service=self
        )

    def get_by_name(self, database_name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        database_id = self.get_id_by_name(database_name=database_name)
        return self.get(database_id=database_id)

    def get_id_by_name(self, database_name: str) -> str:
        """Get a Database id from Firebolt by its name."""
        response = self.client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def list(
        self,
        name_contains: str,
        attached_engine_name_eq: str,
        attached_engine_name_contains: str,
        order_by: Union[str, DatabaseOrder],
    ) -> list[Database]:
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
            url=f"/core/v1/account/databases",
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

    def create(self, database_name: str, region_name: str) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            database_name: Name of the database.
            region_name: Region name in which to create the database.

        Returns:
            The newly created Database.
        """

        class _DatabaseCreateRequest(FireboltBaseModel):
            """Helper model for sending Database creation requests."""

            account_id: str
            database: Database

        region_key = self.resource_manager.regions.get_by_name(
            region_name=region_name
        ).key
        database = Database(name=database_name, compute_region_key=region_key)

        response = self.client.post(
            url=f"/core/v1/accounts/{self.account_id}/databases",
            headers={"Content-type": "application/json"},
            json=_DatabaseCreateRequest(
                account_id=self.account_id,
                database=database,
            ).dict(by_alias=True),
        )
        return Database.parse_obj_with_service(
            obj=response.json()["database"], database_service=self
        )
