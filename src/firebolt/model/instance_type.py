from typing import NamedTuple

from pydantic import BaseModel

from firebolt.firebolt_client import get_firebolt_client


class InstanceTypeId(BaseModel):
    provider_id: str
    region_id: str
    instance_type_id: str


class InstanceType(BaseModel):
    id: InstanceTypeId
    name: str
    is_spot_available: bool
    cpu_virtual_cores_count: int
    memory_size_bytes: str
    storage_size_bytes: str
    price_per_hour_cents: float
    create_time: str
    last_update_time: str


class InstanceTypeLookup(NamedTuple):
    """Lookup an instance type by region and instance type name"""

    region_name: str
    instance_name: str


class InstanceTypes:
    def __init__(self):
        fc = get_firebolt_client()
        response = fc.http_client.get(
            url="/compute/v1/instanceTypes", params={"page.first": 5000}
        )
        self.instance_types = [
            InstanceType.parse_obj(i["node"]) for i in response.json()["edges"]
        ]
        self.instance_types_by_region_name_instance_name = {
            # todo
            InstanceTypeLookup(region_name="todo", instance_name=i.name): i
            for i in self.instance_types
        }
