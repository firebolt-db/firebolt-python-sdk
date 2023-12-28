from re import Pattern
from typing import Callable

from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.common.settings import Settings
from firebolt.model.V1.binding import Binding
from firebolt.model.V1.database import Database
from firebolt.model.V1.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import AlreadyBoundError


def test_get_many_bindings(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    bindings_url: str,
    bindings_callback: Callable,
    settings: Settings,
    mock_engine: Engine,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(bindings_callback, url=bindings_url)

    resource_manager = ResourceManager(settings=settings)
    bindings = resource_manager.bindings.get_many(engine_id=mock_engine.engine_id)
    assert len(bindings) > 0
    assert any(binding.is_default_engine for binding in bindings)


def test_create_binding(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    bindings_url: str,
    binding: Binding,
    create_binding_url: str,
    settings: Settings,
    mock_engine: Engine,
    mock_database: Database,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_response(url=bindings_url, method="GET", json={"edges": []})
    httpx_mock.add_response(
        url=create_binding_url, method="POST", json={"binding": binding.dict()}
    )

    resource_manager = ResourceManager(settings=settings)
    binding = resource_manager.bindings.create(
        engine=mock_engine, database=mock_database, is_default_engine=True
    )
    assert binding.engine_id == mock_engine.engine_id
    assert binding.database_id == mock_database.database_id


def test_create_binding_existing_db(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    bindings_url: str,
    bindings_callback: Callable,
    database_url: str,
    database_callback: Callable,
    settings: Settings,
    mock_engine: Engine,
    mock_database: Database,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(bindings_callback, url=bindings_url)
    httpx_mock.add_callback(database_callback, url=database_url)

    resource_manager = ResourceManager(settings=settings)
    with raises(AlreadyBoundError):
        resource_manager.bindings.create(
            engine=mock_engine, database=mock_database, is_default_engine=True
        )


def test_get_engines_bound_to_db(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_callback: Callable,
    account_id_url: Pattern,
    database_bindings_url: str,
    bindings_database_callback: Callable,
    settings: Settings,
    mock_engine: Engine,
    mock_database: Database,
    engines_by_id_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(bindings_database_callback, url=database_bindings_url)
    httpx_mock.add_response(
        url=engines_by_id_url, method="POST", json={"engines": [mock_engine.dict()]}
    )

    resource_manager = ResourceManager(settings=settings)
    engines = resource_manager.bindings.get_engines_bound_to_database(
        database=mock_database
    )
    assert len(engines) > 0
    assert any(engine.engine_id == mock_engine.engine_id for engine in engines)
