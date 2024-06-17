from typing import Any, Callable, Dict

from httpx import Response, codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import AttachedEngineInUseError


def test_database_create(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    database_get_callback: Callable,
    create_databases_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
    mock_engine: Engine,
):
    httpx_mock.add_callback(
        create_databases_callback, url=system_engine_no_db_query_url, method="POST"
    )
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, method="POST"
    )

    database = resource_manager.databases.create(
        name=mock_database.name,
        description=mock_database.description,
    )

    assert database == mock_database

    for key in ("region", "attached_engines"):
        with raises(ValueError):
            resource_manager.databases.create(name="failed", **{key: "test"})


def test_database_get_by_name(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    database_get_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_database: Database,
):
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, method="POST"
    )

    database = resource_manager.databases.get_by_name(mock_database.name)

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
    )

    assert len(databases) == 2
    assert databases[0] == mock_database
    assert databases[1] == mock_database_2

    for key in (
        "attached_engine_name_eq",
        "attached_engine_name_contains",
        "region_eq",
    ):
        with raises(ValueError):
            resource_manager.databases.get_many(**{key: "value"})


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


def test_database_delete_busy_engine(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    system_engine_no_db_query_url: str,
    get_engine_callback_stopping: Callable,
    mock_database: Database,
):
    httpx_mock.add_callback(
        get_engine_callback_stopping, url=system_engine_no_db_query_url
    )

    mock_database._service = resource_manager.engines

    with raises(AttachedEngineInUseError):
        mock_database.delete()


def test_database_update(
    httpx_mock: HTTPXMock,
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
    mock_database: Database,
    resource_manager: ResourceManager,
    query_statistics: Dict[str, Any],
):
    def update_query_callback(request, **kwargs):
        assert "ALTER DATABASE" in request.read().decode()
        assert "new description" in request.read().decode()
        # Return a dummy response to avoid parsing error
        query_response = {
            "meta": [{"name": "one", "type": "int"}],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    httpx_mock.add_callback(
        update_query_callback, url=system_engine_no_db_query_url, method="POST"
    )
    # Make sure we return an engine with a running state
    mock_database.get_attached_engines = lambda: [mock_engine]

    mocked_service = resource_manager.engines
    mock_database._service = mocked_service

    updated_database = mock_database.update(description="new description")

    assert updated_database.description == "new description"


def test_database_update_with_attached_engine_in_use(
    mock_database: Database,
    mock_engine_stopping: Engine,
):
    # Make sure we return an engine that's not running or stopped
    # to cause an error
    mock_database.get_attached_engines = lambda: [mock_engine_stopping]

    with raises(AttachedEngineInUseError):
        mock_database.update(description="new description")
