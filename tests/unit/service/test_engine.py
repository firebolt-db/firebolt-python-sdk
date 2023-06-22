from typing import Callable

from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import (
    EngineNotFoundError,
    NoAttachedDatabaseError,
)


def test_engine_create(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    get_engine_callback: Callable,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):

    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    engine = resource_manager.engines.create(
        name=mock_engine.name,
        region=mock_engine.region,
        engine_type=mock_engine.type,
        spec=mock_engine.spec,
        scale=mock_engine.scale,
        auto_stop=mock_engine.auto_stop,
        warmup=mock_engine.warmup,
    )

    assert engine == mock_engine


def test_engine_not_found(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    get_engine_not_found_callback: Callable,
    system_engine_no_db_query_url: str,
):
    httpx_mock.add_callback(
        get_engine_not_found_callback, url=system_engine_no_db_query_url
    )

    with raises(EngineNotFoundError):
        resource_manager.engines.get("invalid name")


def test_engine_no_attached_database(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    get_engine_callback: Callable,
    database_not_found_callback: Callable,
    system_engine_no_db_query_url: str,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(
        database_not_found_callback, url=system_engine_no_db_query_url
    )

    engine = resource_manager.engines.get("engine_name")

    with raises(NoAttachedDatabaseError):
        engine.start()


def test_get_connection(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    get_engine_callback: Callable,
    database_get_callback: Callable,
    system_engine_no_db_query_url: str,
    system_engine_query_url: str,
    get_engine_url_callback: Callable,
    mock_query: Callable,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(database_get_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(get_engine_url_callback, url=system_engine_query_url)
    mock_query()

    engine = resource_manager.engines.get("engine_name")

    with engine.get_connection() as connection:
        connection.cursor().execute("select 1")


def test_attach_to_database(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_database: Database,
    mock_engine: Engine,
    get_engine_callback: Callable,
    database_get_callback: Callable,
    attach_engine_to_db_callback: Callable,
    system_engine_no_db_query_url: str,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(database_get_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(
        attach_engine_to_db_callback, url=system_engine_no_db_query_url
    )

    database = resource_manager.databases.get("database")
    engine = resource_manager.engines.get("engine")

    engine._service = resource_manager.engines

    engine.attach_to_database(database)

    assert engine._database_name == database.name


def test_engine_update(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    get_engine_callback: Callable,
    update_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    updated_engine_scale: int,
):
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(update_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    mock_engine._service = resource_manager.engines
    mock_engine.update(scale=updated_engine_scale)

    assert mock_engine.scale == updated_engine_scale
