from functools import cached_property
from typing import NamedTuple, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel, FireboltClientMixin
from firebolt.model.provider import Provider, providers


class RegionKey(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str
    region_id: str

    @property
    def provider(self) -> Provider:
        return providers.get_by_id(provider_id=self.provider_id)


class Region(FireboltBaseModel):
    key: RegionKey = Field(alias="id")
    name: str
    display_name: str
    create_time: str
    last_update_time: str

    @property
    def provider(self) -> Provider:
        return self.key.provider


class RegionLookup(NamedTuple):
    """Helper to lookup a Region by provider name and region name"""

    provider_name: str
    region_name: str


class _Regions(FireboltClientMixin):
    @cached_property
    def regions(self) -> list[Region]:
        response = self.get_firebolt_client().http_client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> dict[RegionLookup, Region]:
        return {
            RegionLookup(provider_name=r.provider.name, region_name=r.name): r
            for r in self.regions
        }

    @cached_property
    def regions_by_key(self) -> dict[RegionKey, Region]:
        return {r.key: r for r in self.regions}

    @cached_property
    def default_region(self) -> Region:
        firebolt_client = self.get_firebolt_client()
        if firebolt_client.default_region_name is None:
            raise ValueError(
                "default_region_name is required. Please set it on FireboltClient or "
                "via environment variable: FIREBOLT_DEFAULT_REGION"
            )
        return self.get_by_name(region_name=firebolt_client.default_region_name)

    def get_by_name(
        self, region_name: str, provider_name: Optional[str] = None
    ) -> Region:
        """Get a region by its name (eg. us-east-1)."""
        if provider_name is None:
            provider_name = providers.default_provider.name

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


regions = _Regions()
