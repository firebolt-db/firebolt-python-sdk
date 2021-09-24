from __future__ import annotations

from datetime import datetime
from typing import Annotated, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel
from firebolt.model.region import RegionKey


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

    @property
    def database_id(self) -> Optional[str]:
        if self.database_key is None:
            return None
        return self.database_key.database_id
