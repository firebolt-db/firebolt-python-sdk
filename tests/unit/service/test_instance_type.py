from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.manager import ResourceManager


def test_instance_types(resource_manager: ResourceManager):
    assert resource_manager.instance_types.instance_types() == [
        InstanceType.S,
        InstanceType.M,
        InstanceType.L,
        InstanceType.XL,
        InstanceType.UNKNOWN,
    ]

    assert resource_manager.instance_types.get("XL") == InstanceType.XL
    assert resource_manager.instance_types.get("XS") == InstanceType.UNKNOWN

    assert resource_manager.instance_types.cheapest_instance() == InstanceType.S
