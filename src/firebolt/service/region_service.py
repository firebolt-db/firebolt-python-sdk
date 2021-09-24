import os
from functools import cached_property
from typing import NamedTuple, Optional

from firebolt.client import FireboltClient
from firebolt.model.region import Region, RegionKey
from firebolt.service.base_service import BaseService
from firebolt.service.provider_service import ProviderService


class RegionLookup(NamedTuple):
    """Helper tuple for looking up Regions by names."""

    provider_name: str
    region_name: str


class RegionService(BaseService):
    DEFAULT_REGION_ENV = "FIREBOLT_DEFAULT_REGION"

    provider_service = None

    def __init__(self, firebolt_client: FireboltClient):
        if self.provider_service is None:
            self.provider_service = ProviderService(firebolt_client=firebolt_client)
        super().__init__(firebolt_client=firebolt_client)

    @cached_property
    def regions(self) -> list[Region]:
        """List of available Regions on Firebolt."""
        response = self.firebolt_client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> dict[RegionLookup, Region]:
        """Dict of {RegionLookup: Region}"""
        return {
            RegionLookup(
                provider_name=self.provider_service.get_by_id(r.key.provider_id),
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
        if self.DEFAULT_REGION_ENV not in os.environ:
            raise ValueError(
                "default_region_name is required. Please set it "
                "via environment variable: FIREBOLT_DEFAULT_REGION"
            )
        return self.get_by_name(region_name=os.environ[self.DEFAULT_REGION_ENV])

    def get_by_name(
        self, region_name: str, provider_name: Optional[str] = None
    ) -> Region:
        """Get a region by its name (eg. us-east-1)."""
        if provider_name is None:
            provider_name = self.provider_service.default_provider.name

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
