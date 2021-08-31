from functools import cached_property
from typing import NamedTuple, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel, FireboltClientMixin
from firebolt.model.provider import Provider
from firebolt.model.region import Region, RegionKey, regions


class InstanceTypeKey(FireboltBaseModel, frozen=True):  # type: ignore
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

    @property
    def provider(self) -> Provider:
        return self.region.provider


class InstanceType(FireboltBaseModel):
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

    @property
    def provider(self) -> Provider:
        return self.region.provider


class InstanceTypeLookup(NamedTuple):
    provider_name: str
    region_name: str
    instance_name: str


class _InstanceTypes(FireboltClientMixin):
    @cached_property
    def instance_types(self) -> list[InstanceType]:
        response = self.get_firebolt_client().http_client.get(
            url="/compute/v1/instanceTypes", params={"page.first": 5000}
        )
        return [InstanceType.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def instance_types_by_key(self) -> dict[InstanceTypeKey, InstanceType]:
        return {i.key: i for i in self.instance_types}

    @cached_property
    def instance_types_by_name(self) -> dict[InstanceTypeLookup, InstanceType]:
        return {
            InstanceTypeLookup(
                provider_name=i.provider.name,
                region_name=i.region.name,
                instance_name=i.name,
            ): i
            for i in self.instance_types
        }

    def get_by_name(
        self,
        instance_name: str,
        region_name: Optional[str] = None,
        provider_name: Optional[str] = None,
    ) -> InstanceType:
        firebolt_client = self.get_firebolt_client()
        if region_name is None:
            if firebolt_client.default_region_name is None:
                raise ValueError("region_name or default_region_name is required.")
            region_name = firebolt_client.default_region_name
        if provider_name is None:
            provider_name = firebolt_client.default_provider_name
        return self.instance_types_by_name[
            InstanceTypeLookup(
                provider_name=provider_name,
                region_name=region_name,
                instance_name=instance_name,
            )
        ]


instance_types = _InstanceTypes()
