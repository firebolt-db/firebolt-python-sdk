from datetime import datetime
from functools import cached_property
from typing import NamedTuple, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel, FireboltClientMixin
from firebolt.model.provider import Provider, providers
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

    # optional
    is_spot_available: Optional[bool]
    cpu_virtual_cores_count: Optional[int]
    memory_size_bytes: Optional[str]
    storage_size_bytes: Optional[str]
    price_per_hour_cents: Optional[float]
    create_time: Optional[datetime]
    last_update_time: Optional[datetime]

    @property
    def region(self) -> Region:
        return self.key.region

    @property
    def provider(self) -> Provider:
        return self.region.provider


class InstanceTypeLookup(NamedTuple):
    """Helper tuple for looking up instance types by names"""

    provider_name: str
    region_name: str
    instance_type_name: str


class _InstanceTypes(FireboltClientMixin):
    @cached_property
    def instance_types(self) -> list[InstanceType]:
        """List of instance types available on Firebolt."""
        response = self.get_firebolt_client().get(
            url="/compute/v1/instanceTypes", params={"page.first": 5000}
        )
        return [InstanceType.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def instance_types_by_key(self) -> dict[InstanceTypeKey, InstanceType]:
        """Dict of {InstanceTypeKey: InstanceType}"""
        return {i.key: i for i in self.instance_types}

    @cached_property
    def instance_types_by_name(self) -> dict[InstanceTypeLookup, InstanceType]:
        """Dict of {InstanceTypeLookup: InstanceType}"""
        return {
            InstanceTypeLookup(
                provider_name=i.provider.name,
                region_name=i.region.name,
                instance_type_name=i.name,
            ): i
            for i in self.instance_types
        }

    def get_by_key(self, instance_type_key: InstanceTypeKey) -> InstanceType:
        """Get an instance type by key."""
        return self.instance_types_by_key[instance_type_key]

    def get_by_name(
        self,
        instance_type_name: str,
        region_name: Optional[str] = None,
        provider_name: str = None,
    ) -> InstanceType:
        """
        Get an instance type by name.

        Args:
            instance_type_name: Name of the instance (eg. "i3.4xlarge").
            region_name:
                Name of the region from which to get the instance.
                If not provided, use the default region name from the client.
            provider_name:
                Name of the provider from which to get the instance.
                If not provided, use the default provider name from the client.

        Returns:
            The requested instance type.
        """

        provider_name = provider_name or providers.default_provider.name
        # Will raise an error if neither set
        region_name = region_name or regions.default_region.name
        return self.instance_types_by_name[
            InstanceTypeLookup(
                provider_name=provider_name,
                region_name=region_name,
                instance_type_name=instance_type_name,
            )
        ]


instance_types = _InstanceTypes()
