from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth
from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager


def test_instance_type(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    server: str,
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_instance_types: List[InstanceType],
    cheapest_instance: InstanceType,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)

    manager = ResourceManager(auth=auth, account_name=account_name, api_endpoint=server)

    assert manager.instance_types.instance_types == mock_instance_types
    assert manager.instance_types.cheapest_instance == cheapest_instance
