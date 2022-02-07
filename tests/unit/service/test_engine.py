from typing import Callable, List

from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.common.exception import FireboltError, NoAttachedDatabaseError
from firebolt.model.engine import Engine
from firebolt.model.instance_type import InstanceType
from firebolt.model.region import Region
from firebolt.service.manager import ResourceManager


def test_engine_create(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_region_1_callback: Callable,
    instance_type_region_1_url: str,
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
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    assert engine.name == engine_name


def test_engine_create_no_available_types(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_empty_callback: Callable,
    instance_type_region_2_url: str,
    settings: Settings,
    mock_instance_types: List[InstanceType],
    engine_name: str,
    account_id_callback: Callable,
    account_id_url: str,
    engine_url: str,
    region_2: Region,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        instance_type_empty_callback, url=instance_type_region_2_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    manager = ResourceManager(settings=settings)

    with raises(FireboltError):
        manager.engines.create(name=engine_name, region=region_2)


def test_engine_no_attached_database(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_region_1_callback: Callable,
    instance_type_region_1_url: str,
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
    account_engine_callback: Callable,
    account_engine_url: str,
    database_callback: Callable,
    database_url: str,
    no_bindings_callback: Callable,
    bindings_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")
    httpx_mock.add_callback(no_bindings_callback, url=bindings_url)

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    with raises(NoAttachedDatabaseError):
        engine.start()


def test_engine_start_binding_to_missing_database(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_region_1_callback: Callable,
    instance_type_region_1_url: str,
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
    database_not_found_callback: Callable,
    database_url: str,
    bindings_callback: Callable,
    bindings_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")
    httpx_mock.add_callback(bindings_callback, url=bindings_url)
    httpx_mock.add_callback(database_not_found_callback, url=database_url)

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    with raises(NoAttachedDatabaseError):
        engine.start()


def test_get_connection(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    instance_type_region_1_callback: Callable,
    instance_type_region_1_url: str,
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
    db_name: str,
    database_callback: Callable,
    database_url: str,
    bindings_callback: Callable,
    bindings_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")
    httpx_mock.add_callback(bindings_callback, url=bindings_url)

    httpx_mock.add_callback(database_callback, url=database_url)

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    with engine.get_connection() as connection:
        assert connection


def test_attach_to_database(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    instance_type_region_1_callback: Callable,
    instance_type_region_1_url: str,
    settings: Settings,
    account_id_callback: Callable,
    account_id_url: str,
    create_databases_callback: Callable,
    databases_url: str,
    database_get_callback: Callable,
    database_get_url: str,
    database_not_found_callback: Callable,
    database_url: str,
    db_name: str,
    engine_name: str,
    engine_callback: Callable,
    engine_url: str,
    create_binding_callback: Callable,
    create_binding_url: str,
    bindings_callback: Callable,
    bindings_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(
        instance_type_region_1_callback, url=instance_type_region_1_url
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(bindings_callback, url=bindings_url)
    httpx_mock.add_callback(create_databases_callback, url=databases_url, method="POST")
    httpx_mock.add_callback(database_not_found_callback, url=database_url, method="GET")

    # create engine
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(engine_callback, url=engine_url, method="POST")

    # attach
    httpx_mock.add_callback(database_get_callback, url=database_get_url)
    httpx_mock.add_callback(
        create_binding_callback, url=create_binding_url, method="POST"
    )

    manager = ResourceManager(settings=settings)
    database = manager.databases.create(name=db_name)

    engine = manager.engines.create(name=engine_name)
    engine.attach_to_database(database=database)

    assert engine.database == database


def test_engine_restart(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    mock_engine: Engine,
    account_id_callback: Callable,
    account_id_url: str,
    engine_callback: Callable,
    account_engine_url: str,
    bindings_callback: Callable,
    bindings_url: str,
    database_callback: Callable,
    database_url: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)

    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    httpx_mock.add_callback(
        engine_callback, url=f"{account_engine_url}:restart", method="POST"
    )
    httpx_mock.add_callback(bindings_callback, url=bindings_url)
    httpx_mock.add_callback(database_callback, url=database_url)

    manager = ResourceManager(settings=settings)

    mock_engine._service = manager.engines
    engine = mock_engine.restart(wait_for_startup=False)

    assert engine.name == mock_engine.name
