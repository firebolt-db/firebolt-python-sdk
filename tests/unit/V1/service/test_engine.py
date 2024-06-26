from re import Pattern
from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.V1.engine import Engine
from firebolt.service.manager import ResourceManager


def test_engine_start_stop(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    mock_engine: Engine,
    account_id_callback: Callable,
    account_id_url: Pattern,
    engine_callback: Callable,
    account_engine_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)

    httpx_mock.add_callback(engine_callback, url=f"{account_engine_url}", method="GET")
    httpx_mock.add_callback(
        engine_callback, url=f"{account_engine_url}:start", method="POST"
    )
    httpx_mock.add_callback(
        engine_callback, url=f"{account_engine_url}:stop", method="POST"
    )

    manager = ResourceManager(settings=settings)

    mock_engine._service = manager.engines
    engine = mock_engine.start(wait_for_startup=False)

    assert engine.name == mock_engine.name

    engine = mock_engine.stop(wait_for_stop=False)

    assert engine.name == mock_engine.name


def test_engine_get_many(
    httpx_mock: HTTPXMock,
    settings: Settings,
    mock_engine: Engine,
    auth_callback: Callable,
    auth_url: str,
    engine_url: str,
    many_engines_callback: Callable,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
):

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    filters = "?page.first=5000&filter.name_contains=test"
    httpx_mock.add_callback(many_engines_callback, url=engine_url + filters)

    manager = ResourceManager(settings=settings)
    engines = manager.engines.get_many(name_contains="test")
    assert len(engines) == 1
    assert engines[0].name == mock_engine.name


def test_engine_get_many_with_region(
    httpx_mock: HTTPXMock,
    settings: Settings,
    mock_engine: Engine,
    auth_callback: Callable,
    auth_url: str,
    engine_url: str,
    many_engines_callback: Callable,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    region_callback: Callable,
    region_url: str,
):

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    filters = "?page.first=5000&filter.name_contains=test&filter.compute_region_id_region_id_eq=mock_region_id_1"
    httpx_mock.add_callback(many_engines_callback, url=engine_url + filters)

    manager = ResourceManager(settings=settings)
    engines = manager.engines.get_many(name_contains="test", region_eq="mock_region_1")
    assert len(engines) == 1
    assert engines[0].name == mock_engine.name
