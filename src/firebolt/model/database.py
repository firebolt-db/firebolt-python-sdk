from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from firebolt.firebolt_client import FireboltClientMixin
from firebolt.model.binding import Binding
from firebolt.model.region import RegionKey

if TYPE_CHECKING:
    from firebolt.model.engine import Engine


class DatabaseKey(BaseModel):
    account_id: str
    database_id: str


class ComputeRegionId(BaseModel):
    provider_id: str
    region_id: str


class Database(BaseModel, FireboltClientMixin):
    database_key: DatabaseKey = Field(alias="id")
    name: str
    description: str
    emoji: str
    compute_region_id: RegionKey
    current_status: str
    health_status: str
    data_size_full: int
    data_size_compressed: int
    is_system_database: bool
    storage_bucket_name: str
    create_time: datetime
    create_actor: str
    last_update_time: datetime
    last_update_actor: str
    desired_status: str

    @property
    def database_id(self) -> str:
        return self.database_key.database_id

    @property
    def engines(self) -> list[Engine]:
        from firebolt.model.engine import Engine

        bindings = Binding.list_bindings(database_id=self.database_id)
        return Engine.get_by_ids([b.engine_id for b in bindings])

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
    def get_id_by_name(cls, database_name: str) -> str:
        """Get a Database id from Firebolt by its name."""
        response = cls.get_firebolt_client().http_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    @classmethod
    def get_by_name(cls, database_name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        database_id = cls.get_id_by_name(database_name=database_name)
        return cls.get_by_id(database_id=database_id)

    def create(self) -> Database:
        """
        Create a Database on Firebolt.

        Returns:
            The newly created Database.
        """
        json_payload = _DatabaseCreateRequest(
            account_id=self.firebolt_client.account_id,
            database=self,
        ).json(by_alias=True)

        response = self.firebolt_client.http_client.post(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/databases",
            headers={"Content-type": "application/json"},
            data=json_payload,
        )

        return Database.parse_obj(response.json()["database"])


class _DatabaseCreateRequest(BaseModel):
    """Helper model for sending Database creation requests."""

    account_id: str
    database: Database
