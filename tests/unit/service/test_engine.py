from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.engine import Engine
from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager


def test_engine_create(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_callback: Callable,
    instance_type_url: str,
    region_callback: Callable,
    region_url: str,
    settings: Settings,
    mock_instance_types: List[InstanceType],
    mock_regions,
    mock_engine: Engine,
    engine_name: str,
    account_id_callback: Callable,
    account_id_url: str,
    engine_callback: Callable,
    engine_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    assert engine.name == engine_name
