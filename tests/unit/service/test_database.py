from re import compile
from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.database import Database
from firebolt.service.manager import ResourceManager


def test_database_create(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    settings: Settings,
    create_databases_callback: Callable,
    databases_url: str,
    db_name: str,
    db_description: str,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(create_databases_callback, url=databases_url, method="POST")

    manager = ResourceManager(settings=settings)
    database = manager.databases.create(name=db_name, description=db_description)

    assert database.name == db_name
    assert database.description == db_description


def test_database_get_by_name(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    database_get_by_name_callback: Callable,
    database_get_by_name_url: str,
    database_get_callback: Callable,
    database_get_url: str,
    mock_database: Database,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(database_get_by_name_callback, url=database_get_by_name_url)
    httpx_mock.add_callback(database_get_callback, url=database_get_url)

    manager = ResourceManager(settings=settings)
    database = manager.databases.get_by_name(name=mock_database.name)

    assert database.name == mock_database.name


def test_database_get_many(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    database_get_by_name_callback: Callable,
    database_get_by_name_url: str,
    databases_get_callback: Callable,
    databases_url: str,
    mock_database: Database,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        databases_get_callback,
        url=compile(databases_url + "?[a-zA-Z0-9=&]*"),
        method="GET",
    )

    manager = ResourceManager(settings=settings)
    databases = manager.databases.get_many(
        name_contains=mock_database.name,
        attached_engine_name_eq="mockengine",
        attached_engine_name_contains="mockengine",
    )

    assert len(databases) == 1
    assert databases[0].name == mock_database.name


def test_database_update(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    database_update_callback: Callable,
    database_url: str,
    mock_database: Database,
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)

    httpx_mock.add_callback(database_update_callback, url=database_url, method="PATCH")

    manager = ResourceManager(settings=settings)

    mock_database._service = manager.databases
    database = mock_database.update(description="new description")

    assert database.description == "new description"
