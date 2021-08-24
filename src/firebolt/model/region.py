from enum import Enum

from pydantic import BaseModel


class RegionEnum(Enum):
    us_east_1 = "us-east-1"
    eu_west_1 = "eu-west-1"


class Region(BaseModel):
    provider_id: str
    region_id: str
    name: RegionEnum

    @classmethod
    def get_by_name(cls, name: str):
        region_enum = RegionEnum(name)
        if region_enum == RegionEnum.us_east_1:
            return US_EAST_1
        if region_enum == RegionEnum.eu_west_1:
            return EU_WEST_1

    @classmethod
    def get_by_id(cls, region_id: str):
        # todo
        return ""


# as long as we only support a few regions, these can be hardcoded
# when we support more, this should be refactored to call the API
US_EAST_1 = Region(
    provider_id="402a51bb-1c8e-4dc4-9e05-ced3c1e2186e",
    region_id="f1841f9f-4031-4a9a-b3d7-1dc27e7e61ed",
    name="us-east-1",
)

EU_WEST_1 = Region(
    provider_id="402a51bb-1c8e-4dc4-9e05-ced3c1e2186e",
    region_id="fcacdb84-5206-4f5c-99b5-75668e1f53fb",
    name="eu-west-1",
)
