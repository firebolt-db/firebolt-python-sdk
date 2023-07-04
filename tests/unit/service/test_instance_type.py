from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager


def test_instance_type(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_instance_types: List[InstanceType],
    cheapest_instance: InstanceType,
    mock_system_engine_connection_flow: Callable,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)

    assert resource_manager.instance_types.instance_types == mock_instance_types
    assert resource_manager.instance_types.cheapest_instance == cheapest_instance
