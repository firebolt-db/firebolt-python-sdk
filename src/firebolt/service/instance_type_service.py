from functools import cached_property
from typing import NamedTuple, Optional

from firebolt.model.instance_type import InstanceType, InstanceTypeKey
from firebolt.service.base_service import BaseService


class InstanceTypeLookup(NamedTuple):
    """Helper tuple for looking up instance types by names"""

    provider_name: str
    region_name: str
    instance_type_name: str


class InstanceTypeService(BaseService):
    @cached_property
    def instance_types(self) -> list[InstanceType]:
        """List of instance types available on Firebolt."""
        response = self.firebolt_client.get(
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
                provider_name=self.firebolt_client.provider_service.get_by_id(
                    provider_id=i.key.provider_id
                ).name,
                region_name=self.firebolt_client.region_service.get_by_id(
                    region_id=i.key.region_id
                ).name,
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
        provider_name = (
            provider_name or self.firebolt_client.provider_service.default_provider.name
        )
        # Will raise an error if neither set
        region_name = (
            region_name or self.firebolt_client.region_service.default_region.name
        )
        return self.instance_types_by_name[
            InstanceTypeLookup(
                provider_name=provider_name,
                region_name=region_name,
                instance_type_name=instance_type_name,
            )
        ]
