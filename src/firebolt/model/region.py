from functools import cached_property

from pydantic import BaseModel, Field

from firebolt.common.base_model import HashableBaseModel
from firebolt.firebolt_client import FireboltClientMixin


class RegionKey(HashableBaseModel):
    provider_id: str
    region_id: str


class Region(BaseModel):
    key: RegionKey = Field(alias="id")
    name: str
    display_name: str
    create_time: str
    last_update_time: str


class Regions(FireboltClientMixin):
    @cached_property
    def regions(self) -> list[Region]:
        response = self.firebolt_client.http_client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return [Region.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def regions_by_name(self) -> dict[str, Region]:
        return {r.name: r for r in self.regions}

    @cached_property
    def regions_by_key(self) -> dict[RegionKey, Region]:
        return {r.key: r for r in self.regions}

    @cached_property
    def default_region(self) -> Region:
        return self.regions_by_name[self.firebolt_client.default_region_name]

    def get_by_name(self, region_name: str):
        return self.regions_by_name[region_name]

    def get_by_key(self, region_key: RegionKey):
        return self.regions_by_key[region_key]

    def get_by_region_id(self, region_id: str, provider_id: str = None):
        if provider_id is None:
            provider_id = self.default_region.key.provider_id
        return self.regions_by_key[
            RegionKey(provider_id=provider_id, region_id=region_id)
        ]


regions = Regions()
