from re import Pattern
from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common.settings import Settings
from firebolt.model.V1.engine import Engine
from firebolt.service.manager import ResourceManager


def test_get_many_bindings(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    bindings_url: str,
    bindings_callback: Callable,
    settings: Settings,
    mock_engine: Engine,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(bindings_callback, url=bindings_url)

    resource_manager = ResourceManager(settings=settings)
    bindings = resource_manager.bindings.get_many(engine_id=mock_engine.engine_id)
    assert len(bindings) > 0
    assert any(binding.is_default_engine for binding in bindings)
