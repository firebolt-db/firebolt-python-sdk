from functools import cached_property

from firebolt.model.region import Region, RegionKey
from firebolt.service.base import BaseService
from firebolt.service.manager import ResourceManager


class RegionService(BaseService):
    def __init__(
        self, resource_manager: ResourceManager, default_region_name: str = None
    ):
        """
        Service to manage AWS Regions (us-east-1, etc).

        Args:
            resource_manager: Resource manager to use.
            default_region_name: AWS Region to use as a default.
        """
        self.default_region_name = default_region_name
        super().__init__(resource_manager=resource_manager)

    @cached_property
    def regions(self) -> list[Region]:
        """List of available AWS Regions on Firebolt."""
        response = self.client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> dict[str, Region]:
        """Dict of {RegionLookup: Region}"""
        return {r.name: r for r in self.regions}

    @cached_property
    def regions_by_key(self) -> dict[RegionKey, Region]:
        """Dict of {RegionKey: Region}"""
        return {r.key: r for r in self.regions}

    @cached_property
    def default_region(self) -> Region:
        """Default AWS Region, could be provided from environment."""

        if not self.default_region_name:
            raise ValueError(
                "The environment variable FIREBOLT_DEFAULT_REGION must be set."
            )
        return self.get_by_name(region_name=self.default_region_name)

    def get_by_name(self, region_name: str) -> Region:
        """Get an AWS region by its name (eg. us-east-1)."""
        return self.regions_by_name[region_name]

    def get_by_key(self, region_key: RegionKey) -> Region:
        """Get an AWS Region by its key."""
        return self.regions_by_key[region_key]

    def get_by_id(self, region_id: str) -> Region:
        """Get an AWS Region by region_id."""
        return self.get_by_key(
            RegionKey(
                provider_id=self.resource_manager.provider_id, region_id=region_id
            )
        )
