from functools import cached_property
from typing import NamedTuple, Optional

from firebolt.model.region import Region, RegionKey
from firebolt.service.base import BaseService
from firebolt.service.manager import ResourceManager


class RegionLookup(NamedTuple):
    """Helper tuple for looking up Regions by names."""

    provider_name: str
    region_name: str


class RegionService(BaseService):
    def __init__(
        self, resource_manager: ResourceManager, default_region_name: str = None
    ):
        """
        Service to manage Regions (us-east-1, etc).

        Args:
            resource_manager: Resource manager to use.
            default_region_name: Region to use as a default.
        """
        self.default_region_name = default_region_name
        super().__init__(resource_manager=resource_manager)

    @cached_property
    def regions(self) -> list[Region]:
        """List of available Regions on Firebolt."""
        response = self.client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> dict[RegionLookup, Region]:
        """Dict of {RegionLookup: Region}"""
        return {
            RegionLookup(
                provider_name=self.resource_manager.providers.get_by_id(
                    r.key.provider_id
                ).name,
                region_name=r.name,
            ): r
            for r in self.regions
        }

    @cached_property
    def regions_by_key(self) -> dict[RegionKey, Region]:
        """Dict of {RegionKey: Region}"""
        return {r.key: r for r in self.regions}

    @cached_property
    def default_region(self) -> Region:
        """Default Region, could be provided from environment."""

        if not self.default_region_name:
            raise ValueError(
                "The environment variable FIREBOLT_DEFAULT_REGION must be set."
            )
        return self.get_by_name(region_name=self.default_region_name)

    def get_by_name(
        self, region_name: str, provider_name: Optional[str] = None
    ) -> Region:
        """Get a region by its name (eg. us-east-1)."""
        assert self.resource_manager.providers is not None
        if provider_name is None:
            provider_name = self.resource_manager.providers.default_provider.name

        return self.regions_by_name[
            RegionLookup(provider_name=provider_name, region_name=region_name)
        ]

    def get_by_key(self, region_key: RegionKey) -> Region:
        """Get a Region by its key."""
        return self.regions_by_key[region_key]

    def get_by_id(self, region_id: str, provider_id: str = None) -> Region:
        """Get a Region by region_id and provider id."""
        if provider_id is None:
            provider_id = self.default_region.key.provider_id
        return self.get_by_key(RegionKey(provider_id=provider_id, region_id=region_id))
