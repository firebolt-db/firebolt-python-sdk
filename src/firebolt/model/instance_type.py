from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel


class InstanceTypeKey(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str
    region_id: str
    instance_type_id: str


class InstanceType(FireboltBaseModel):
    key: InstanceTypeKey = Field(alias="id")
    name: str

    # optional
    is_spot_available: Optional[bool]
    cpu_virtual_cores_count: Optional[int]
    memory_size_bytes: Optional[int]
    storage_size_bytes: Optional[int]
    price_per_hour_cents: Optional[float]
    create_time: Optional[datetime]
    last_update_time: Optional[datetime]
