from typing import Callable

from httpx import Request
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine, EngineStatus
from firebolt.service.manager import ResourceManager
from firebolt.service.V2.types import EngineType
from firebolt.utils.exception import (
    EngineNotFoundError,
    NoAttachedDatabaseError,
)

from tests.unit.response import Response
from tests.unit.service.V2.conftest import get_objects_from_db_callback


def test_engine_create(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    def create_engine_callback(request: Request) -> Response:
        if request.content.startswith(b"CREATE"):
            assert (
                request.content.decode("utf-8")
                == "CREATE ENGINE engine_1 WITH REGION = 'us-east-1' ENGINE_TYPE = 'GENERAL_PURPOSE'"
                " SPEC = 'B1' SCALE = 2 AUTO_STOP = 7200 WARMUP = 'MINIMAL'"
            )
        return get_objects_from_db_callback([mock_engine])(request)

    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(create_engine_callback, url=system_engine_no_db_query_url)

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
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_engine: Engine,
    get_engine_callback: Callable,
    update_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    updated_engine_scale: int,
    updated_engine_type: EngineType,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(update_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    mock_engine._service = resource_manager.engines
    mock_engine.update(scale=updated_engine_scale, engine_type=updated_engine_type)

    assert mock_engine.scale == updated_engine_scale
    assert mock_engine.type == updated_engine_type

    mock_engine.update(scale=updated_engine_scale, engine_type=updated_engine_type)

    assert mock_engine.scale == updated_engine_scale
    assert mock_engine.type == updated_engine_type


def test_engine_update_auto_stop_zero(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    mock_engine: Engine,
    get_engine_callback: Callable,
    update_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    updated_auto_stop: int,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(update_engine_callback, url=system_engine_no_db_query_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    mock_engine.auto_stop = updated_auto_stop + 100
    # auto_stop = 0 is not considered an empty parameter value
    mock_engine._service = resource_manager.engines
    mock_engine.update(auto_stop=0)

    assert mock_engine.auto_stop == updated_auto_stop


def test_engine_get_by_name(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    get_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
):
    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    engine = resource_manager.engines.get_by_name(mock_engine.name)

    assert engine == mock_engine


def test_engine_deleteting(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    instance_type_callback: Callable,
    instance_type_url: str,
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
):
    mock_engine.current_status = "Deleting"
    get_engine_callback = get_objects_from_db_callback([mock_engine])

    httpx_mock.add_callback(instance_type_callback, url=instance_type_url)
    httpx_mock.add_callback(get_engine_callback, url=system_engine_no_db_query_url)

    engine = resource_manager.engines.get_by_name(mock_engine.name)

    assert engine.current_status == EngineStatus.DELETING
