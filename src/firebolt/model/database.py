from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel
from firebolt.model.binding import Binding
from firebolt.model.region import RegionKey

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

    @property
    def database_id(self) -> Optional[str]:
        if self.database_key is None:
            return None
        return self.database_key.database_id

    @property
    def engines(self) -> list[Engine]:
        """Engines bound to this database."""
        from firebolt.model.engine import Engine

        # todo: use binding/engine services instead
        bindings = Binding.list_bindings(database_id=self.database_id)
        return Engine.get_by_ids([b.engine_id for b in bindings])
