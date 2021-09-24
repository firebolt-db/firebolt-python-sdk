from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel


class InstanceTypeKey(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str
    region_id: str
    instance_type_id: str

    # @property
    # def region(self) -> Region:
    #     return regions.get_by_key(
    #         RegionKey(
    #             provider_id=self.provider_id,
    #             region_id=self.region_id,
    #         )
    #     )
    #
    # @property
    # def provider(self) -> Provider:
    #     return self.region.provider


class InstanceType(FireboltBaseModel):
    key: InstanceTypeKey = Field(alias="id")
    name: str

    # optional
    is_spot_available: Optional[bool]
    cpu_virtual_cores_count: Optional[int]
    memory_size_bytes: Optional[str]
    storage_size_bytes: Optional[str]
    price_per_hour_cents: Optional[float]
    create_time: Optional[datetime]
    last_update_time: Optional[datetime]

    # @property
    # def region(self) -> Region:
    #     return self.key.region
    #
    # @property
    # def provider(self) -> Provider:
    #     return self.region.provider
