from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from firebolt.model import FireboltBaseModel


class BindingKey(BaseModel):
    account_id: str
    database_id: str
    engine_id: str


class Binding(FireboltBaseModel):
    """A binding between an engine and a database."""

    binding_key: BindingKey = Field(alias="id")
    is_default_engine: bool = Field(alias="engine_is_default")

    # optional
    current_status: Optional[str]
    health_status: Optional[str]
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    desired_status: Optional[str]

    @property
    def database_id(self) -> str:
        return self.binding_key.database_id

    @property
    def engine_id(self) -> str:
        return self.binding_key.engine_id
