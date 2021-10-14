from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Optional

from pydantic import Field, PrivateAttr

from firebolt.model import FireboltBaseModel
from firebolt.model.region import RegionKey

if TYPE_CHECKING:
    from firebolt.service.database import DatabaseService


class DatabaseKey(FireboltBaseModel):
    account_id: str
    database_id: str


class Database(FireboltBaseModel):
    """
    A Firebolt database.

    Databases belong to a region and have a description,
    but otherwise are not configurable.
    """

    # internal
    _database_service: DatabaseService = PrivateAttr()

    # required
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

    @classmethod
    def parse_obj_with_service(
        cls, obj: Any, database_service: DatabaseService
    ) -> Database:
        database = cls.parse_obj(obj)
        database._database_service = database_service
        return database

    @property
    def database_id(self) -> Optional[str]:
        if self.database_key is None:
            return None
        return self.database_key.database_id

    def delete(self, database_id: str) -> Database:
        """Delete a database from Firebolt."""
        response = self._database_service.client.delete(
            url=f"/core/v1/account/databases/{database_id}",
            headers={"Content-type": "application/json"},
        )
        return Database.parse_obj_with_service(
            response.json()["database"], self._database_service
        )
