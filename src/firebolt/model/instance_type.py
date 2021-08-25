from functools import cached_property
from typing import NamedTuple

from pydantic import BaseModel, Field

from firebolt.firebolt_client import FireboltClientMixin
from firebolt.model.region import Region, RegionKey, regions


class InstanceTypeKey(BaseModel):
    provider_id: str
    region_id: str
    instance_type_id: str

    @property
    def region(self) -> Region:
        return regions.get_by_key(
            RegionKey(
                provider_id=self.provider_id,
                region_id=self.region_id,
            )
        )


class InstanceType(BaseModel):
    key: InstanceTypeKey = Field(alias="id")
    name: str
    is_spot_available: bool
    cpu_virtual_cores_count: int
    memory_size_bytes: str
    storage_size_bytes: str
    price_per_hour_cents: float
    create_time: str
    last_update_time: str

    @property
    def region(self) -> Region:
        return self.key.region


class InstanceTypeLookup(NamedTuple):
    """
    Lookup an instance type by region and instance type name
    Consider replacing this with a frozen pydantic model (currently a beta feature)
        or a HashableBaseModel
    """

    region_name: str
    instance_name: str


class _InstanceTypes(FireboltClientMixin):
    @cached_property
    def instance_types(self) -> list[InstanceType]:
        response = self.firebolt_client.http_client.get(
            url="/compute/v1/instanceTypes", params={"page.first": 5000}
        )
        return [InstanceType.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def instance_types_by_region_name_instance_name(
        self,
    ) -> dict[InstanceTypeLookup, InstanceType]:
        return {
            InstanceTypeLookup(region_name=i.region.name, instance_name=i.name): i
            for i in self.instance_types
        }

    def get_by_region_name_instance_name(self, instance_name: str, region_name: str):
        return self.instance_types_by_region_name_instance_name[
            InstanceTypeLookup(
                region_name=region_name,
                instance_name=instance_name,
            )
        ]


instance_types = _InstanceTypes()
