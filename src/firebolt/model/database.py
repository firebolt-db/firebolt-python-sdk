from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.region import RegionKey, regions

if TYPE_CHECKING:
    from firebolt.model.engine import Engine


class DatabaseKey(FireboltBaseModel):
    account_id: str
    database_id: str


class Database(FireboltBaseModel):
    name: Annotated[str, Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")]
    compute_region_key: RegionKey = Field(alias="compute_region_id")

    # optional
    database_key: Optional[DatabaseKey] = Field(alias="id")
    description: Optional[Annotated[str, Field(max_length=255)]]
    emoji: Optional[Annotated[str, Field(max_length=255)]]
    current_status: Optional[str]
    health_status: Optional[str]
    data_size_full: Optional[int]
    data_size_compressed: Optional[int]
    is_system_database: Optional[bool]
    storage_bucket_name: Optional[str]
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    desired_status: Optional[str]

    class Config:
        allow_population_by_field_name = True

    @classmethod
    def get_by_id(cls, database_id: str) -> Database:
        """Get a Database from Firebolt by its id."""
        fc = cls.get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/databases/{database_id}",
        )
        database_spec: dict = response.json()["database"]
        return Database.parse_obj(database_spec)

    @classmethod
    def get_by_name(cls, database_name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        database_id = cls.get_id_by_name(database_name=database_name)
        return cls.get_by_id(database_id=database_id)

    @classmethod
    def get_id_by_name(cls, database_name: str) -> str:
        """Get a Database id from Firebolt by its name."""
        response = cls.get_firebolt_client().http_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    @classmethod
    def create_new(cls, database_name: str, region_name: str) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            database_name: Name of the database.
            region_name: Region name in which to create the database.

        Returns:
            The newly created Database.
        """
        region_key = regions.get_by_name(region_name=region_name).key
        database = Database(name=database_name, compute_region_key=region_key)
        return database.apply_create()

    @property
    def database_id(self) -> Optional[str]:
        if self.database_key is None:
            return None
        return self.database_key.database_id

    @property
    def engines(self) -> list[Engine]:
        """Engines bound to this database."""
        from firebolt.model.engine import Engine

        bindings = Binding.list_bindings(database_id=self.database_id)
        return Engine.get_by_ids([b.engine_id for b in bindings])

    def apply_create(self) -> Database:
        """
        Create a Database on Firebolt from the local Database object.

        Returns:
            The newly created Database.
        """
        firebolt_client = self.get_firebolt_client()
        response = firebolt_client.http_client.post(
            url=f"/core/v1/accounts/{firebolt_client.account_id}/databases",
            headers={"Content-type": "application/json"},
            json=_DatabaseCreateRequest(
                account_id=self.get_firebolt_client().account_id,
                database=self,
            ).dict(by_alias=True),
        )
        return Database.parse_obj(response.json()["database"])

    def delete(self) -> Database:
        """Delete a database from Firebolt."""
        response = self.get_firebolt_client().http_client.delete(
            url=f"/core/v1/account/databases/{self.database_id}",
            headers={"Content-type": "application/json"},
        )
        return Database.parse_obj(response.json()["database"])


class _DatabaseCreateRequest(FireboltBaseModel):
    """Helper model for sending Database creation requests."""

    account_id: str
    database: Database
