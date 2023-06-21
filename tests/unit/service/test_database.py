from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.model.database import Database
from firebolt.service.manager import ResourceManager


def test_database_create(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    database_get_callback: Callable,
    create_databases_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
):
    httpx_mock.add_callback(
        create_databases_callback, url=system_engine_no_db_query_url, method="POST"
    )
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, method="POST"
    )

    database = resource_manager.databases.create(
        name=mock_database.name, description=mock_database.description
    )

    assert database == mock_database


def test_database_get(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    database_get_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
):
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, method="POST"
    )

    database = resource_manager.databases.get(mock_database.name)

    assert database == mock_database


def test_database_get_many(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    databases_get_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
    mock_database_2: Database,
):
    httpx_mock.add_callback(
        databases_get_callback,
        url=system_engine_no_db_query_url,
        method="POST",
    )

    databases = resource_manager.databases.get_many(
        name_contains=mock_database.name,
        attached_engine_name_eq="mockengine",
        attached_engine_name_contains="mockengine",
        region_eq="us-east-1",
    )

    assert len(databases) == 2
    assert databases[0] == mock_database
    assert databases[1] == mock_database_2


def test_database_update(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    database_update_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
):
    httpx_mock.add_callback(
        database_update_callback, url=system_engine_no_db_query_url, method="POST"
    )

    mock_database._service = resource_manager.databases
    database = mock_database.update(description="new description")

    assert database.description == "new description"
