from typing import Dict, List

from firebolt.model.region import Region, RegionKey
from firebolt.service.base import BaseService
from firebolt.service.manager import ResourceManager
from firebolt.utils.urls import REGIONS_URL
from firebolt.utils.util import cached_property


class RegionService(BaseService):
    def __init__(self, resource_manager: ResourceManager):
        """
        Service to manage AWS regions (us-east-1, etc)

        Args:
            resource_manager: Resource manager to use
        """

        super().__init__(resource_manager=resource_manager)

    @cached_property
    def regions(self) -> List[Region]:
        """List of available AWS regions on Firebolt."""

        response = self.client.get(url=REGIONS_URL, params={"page.first": 5000})
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> Dict[str, Region]:
        """Dict of {RegionLookup to Region}"""

        return {r.name: r for r in self.regions}

    @cached_property
    def regions_by_key(self) -> Dict[RegionKey, Region]:
        """Dict of {RegionKey to Region}"""

        return {r.key: r for r in self.regions}

    @cached_property
    def default_region(self) -> Region:
        """Default AWS region, could be provided from environment."""

        if not self.settings.default_region:
            raise ValueError(
                "The environment variable FIREBOLT_DEFAULT_REGION must be set."
            )
        return self.get_by_name(name=self.settings.default_region)

    def get_by_name(self, name: str) -> Region:
        """Get an AWS region by its name (eg. us-east-1)."""

        return self.regions_by_name[name]

    def get_by_key(self, key: RegionKey) -> Region:
        """Get an AWS region by its key."""

        return self.regions_by_key[key]

    def get_by_id(self, id_: str) -> Region:
        """Get an AWS region by region_id."""

        return self.get_by_key(
            RegionKey(provider_id=self.resource_manager.provider_id, region_id=id_)
        )
