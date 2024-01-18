from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.model.V1 import FireboltBaseModel


class RegionKey(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str
    region_id: str


class Region(FireboltBaseModel):
    key: RegionKey = Field(alias="id")
    name: str

    # optional
    display_name: Optional[str] = None
    create_time: Optional[datetime] = None
    last_update_time: Optional[datetime] = None
