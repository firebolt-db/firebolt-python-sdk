from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.instance_type import InstanceType
from firebolt.model.region import Region
from firebolt.service.manager import ResourceManager


def test_instance_type(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    instance_type_callback: Callable,
    instance_type_region_1_callback: Callable,
    instance_type_empty_callback: Callable,
    instance_type_url: str,
    instance_type_region_1_url: str,
    instance_type_region_2_url: str,
    settings: Settings,
    mock_instance_types: List[InstanceType],
    cheapest_instance: InstanceType,
    region_1: Region,
    region_2: Region,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(
        instance_type_empty_callback, url=instance_type_region_2_url
    )

    manager = ResourceManager(settings=settings)
    assert manager.instance_types.instance_types == mock_instance_types
    assert (
        manager.instance_types.cheapest_instance_in_region(region_1)
        == cheapest_instance
    )
    assert not manager.instance_types.cheapest_instance_in_region(region_2)
