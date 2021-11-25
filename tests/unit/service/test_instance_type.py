from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.instance_type import InstanceType
from firebolt.service.instance_type import HasStorage
from firebolt.service.manager import ResourceManager


def test_instance_type(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_callback: Callable,
    instance_type_url: str,
    account_id_callback: Callable,
    account_id_url: str,
    settings: Settings,
    mock_instance_types: List[InstanceType],
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)

    manager = ResourceManager(settings=settings)
    assert manager.instance_types.instance_types == mock_instance_types

    # Make sure test conditions are met
    assert (
        mock_instance_types[0].price_per_hour_cents
        < mock_instance_types[1].price_per_hour_cents
    )
    assert not mock_instance_types[2].price_per_hour_cents

    # Get cheapest instance
    assert manager.instance_types.get_cheapest() == mock_instance_types[0]

    # Make sure test conditions are met
    assert (
        not mock_instance_types[0].storage_size_bytes
        or mock_instance_types[0].storage_size_bytes == "0"
    )
    # Get cheapest instance that also has storage
    assert (
        manager.instance_types.get_cheapest(filters=[HasStorage])
        == mock_instance_types[1]
    )
