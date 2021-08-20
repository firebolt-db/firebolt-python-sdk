from __future__ import annotations

from typing import TYPE_CHECKING

from firebolt.model.database import Database

if TYPE_CHECKING:  # prevent circular imports
    from firebolt.firebolt_client import FireboltClient


class DatabaseService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client
        self.http_client = firebolt_client.http_client

    def get_id_by_name(self, database_name: str) -> str:
        response = self.http_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def get_by_id(self, database_id: str) -> Database:
        response = self.http_client.get(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/databases/{database_id}",
        )
        database_spec: dict = response.json()["database"]
        return Database.parse_obj(database_spec)

    def get_by_name(self, database_name: str) -> Database:
        database_id = self.get_id_by_name(database_name=database_name)
        return self.get_by_id(database_id=database_id)
