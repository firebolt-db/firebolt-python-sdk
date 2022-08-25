import logging
from typing import List, Optional, Union

from firebolt.model import FireboltBaseModel
from firebolt.model.database import Database
from firebolt.service.base import BaseService
from firebolt.service.types import DatabaseOrder
from firebolt.utils.urls import (
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
)
from firebolt.utils.util import prune_dict

logger = logging.getLogger(__name__)


class DatabaseService(BaseService):
    def get(self, id_: str) -> Database:
        """Get a Database from Firebolt by its ID."""

        response = self.client.get(
            url=ACCOUNT_DATABASE_URL.format(account_id=self.account_id, database_id=id_)
        )
        return Database.parse_obj_with_service(
            obj=response.json()["database"], database_service=self
        )

    def get_by_name(self, name: str) -> Database:
        """Get a database from Firebolt by its name."""

        database_id = self.get_id_by_name(name=name)
        return self.get(id_=database_id)

    def get_id_by_name(self, name: str) -> str:
        """Get a database ID from Firebolt by its name."""

        response = self.client.get(
            url=ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=self.account_id),
            params={"database_name": name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def get_many(
        self,
        name_contains: Optional[str] = None,
        attached_engine_name_eq: Optional[str] = None,
        attached_engine_name_contains: Optional[str] = None,
        order_by: Optional[Union[str, DatabaseOrder]] = None,
    ) -> List[Database]:
        """
        Get a list of databases on Firebolt.

        Args:
            name_contains: Filter for databases with a name containing this substring
            attached_engine_name_eq: Filter for databases by an exact engine name
            attached_engine_name_contains: Filter for databases by engines with a
                name containing this substring
            order_by: Method by which to order the results.
                See :py:class:`firebolt.service.types.DatabaseOrder`

        Returns:
            A list of databases matching the filters
        """

        if isinstance(order_by, str):
            order_by = DatabaseOrder[order_by].name

        params = {
            "page.first": "1000",
            "order_by": order_by,
            "filter.name_contains": name_contains,
            "filter.attached_engine_name_eq": attached_engine_name_eq,
            "filter.attached_engine_name_contains": attached_engine_name_contains,
        }

        response = self.client.get(
            url=ACCOUNT_DATABASES_URL.format(account_id=self.account_id),
            params=prune_dict(params),
        )

        return [
            Database.parse_obj_with_service(obj=d["node"], database_service=self)
            for d in response.json()["edges"]
        ]

    def create(
        self, name: str, region: Optional[str] = None, description: Optional[str] = None
    ) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            name: Name of the database
            region: Region name in which to create the database

        Returns:
            The newly created database
        """

        class _DatabaseCreateRequest(FireboltBaseModel):
            """Helper model for sending database creation requests."""

            account_id: str
            database: Database

        if region is None:
            region_key = self.resource_manager.regions.default_region.key
        else:
            region_key = self.resource_manager.regions.get_by_name(name=region).key
        database = Database(
            name=name, compute_region_key=region_key, description=description
        )

        logger.info(f"Creating Database (name={name})")
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
