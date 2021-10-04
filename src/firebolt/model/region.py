from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.common.constants import AWS_PROVIDER_ID
from firebolt.model import FireboltBaseModel


class RegionKey(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str = AWS_PROVIDER_ID
    region_id: str


class Region(FireboltBaseModel):
    key: RegionKey = Field(alias="id")
    name: str

    # optional
    display_name: Optional[str]
    create_time: Optional[datetime]
    last_update_time: Optional[datetime]
