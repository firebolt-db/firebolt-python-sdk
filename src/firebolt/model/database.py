from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from firebolt.firebolt_client import FireboltClientMixin
from firebolt.model.region import RegionKey


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

    # def get_id_by_name(self, database_name: str) -> str:
    #     response = self.firebolt_client.http_client.get(
    #         url=f"/core/v1/account/databases:getIdByName",
    #         params={"database_name": database_name},
    #     )
    #     database_id = response.json()["database_id"]["database_id"]
    #     return database_id

    @classmethod
    def get_by_id(cls, database_id: str) -> Database:
        fc = cls.get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/databases/{database_id}",
        )
        database_spec: dict = response.json()["database"]
        return Database.parse_obj(database_spec)

    # def get_by_name(self, database_name: str) -> Database:
    #     database_id = self.get_id_by_name(database_name=database_name)
    #     return self.get_by_id(database_id=database_id)
