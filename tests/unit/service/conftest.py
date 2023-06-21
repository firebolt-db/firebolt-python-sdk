import json
from datetime import datetime
from typing import Callable, List
from urllib.parse import urlparse

import httpx
from httpx import Response
from pytest import fixture

from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.model.instance_type import InstanceType
from firebolt.utils.urls import (
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_INSTANCE_TYPES_URL,
    ACCOUNT_LIST_ENGINES_URL,
)
from tests.unit.util import list_to_paginated_response


@fixture
def engine_name() -> str:
    return "my_engine"


@fixture
def engine_scale() -> int:
    return 2


@fixture
def mock_engine(engine_name, region_1, engine_settings, account_id, server) -> Engine:
    return Engine(
        name=engine_name,
        compute_region_key=region_1.key,
        settings=engine_settings,
        key=EngineKey(account_id=account_id, engine_id="mock_engine_id_1"),
        endpoint=f"https://{server}",
    )


@fixture
def instance_type_1() -> InstanceType:
    return InstanceType(
        name="B1",
        price_per_hour_cents=40,
        storage_size_bytes=0,
        is_spot_available=True,
        cpu_virtual_cores_count=0,
        memory_size_bytes=0,
        create_time=datetime.now().isoformat(),
        last_update_time=datetime.now().isoformat(),
        _key={},
        _service=None,
    )


@fixture
def instance_type_2() -> InstanceType:
    return InstanceType(
        name="B2",
        price_per_hour_cents=20,
        storage_size_bytes=500,
        is_spot_available=True,
        cpu_virtual_cores_count=0,
        memory_size_bytes=0,
        create_time=datetime.now().isoformat(),
        last_update_time=datetime.now().isoformat(),
        _key={},
        _service=None,
    )


@fixture
def instance_type_3() -> InstanceType:
    return InstanceType(
        name="B3",
        price_per_hour_cents=30,
        storage_size_bytes=500,
        is_spot_available=True,
        cpu_virtual_cores_count=0,
        memory_size_bytes=0,
        create_time=datetime.now().isoformat(),
        last_update_time=datetime.now().isoformat(),
        _key={},
        _service=None,
    )


@fixture
def cheapest_instance(instance_type_2) -> InstanceType:
    return instance_type_2


@fixture
def mock_instance_types(
    instance_type_1, instance_type_2, instance_type_3
) -> List[InstanceType]:
    return [instance_type_1, instance_type_2, instance_type_3]


@fixture
def instance_type_callback(instance_type_url: str, mock_instance_types) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == instance_type_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_instance_types),
        )

    return do_mock


@fixture
def instance_type_empty_callback() -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([]),
        )

    return do_mock


@fixture
def instance_type_url(server: str, account_id: str) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_INSTANCE_TYPES_URL.format(account_id=account_id)
        + "?page.first=5000"
    )


@fixture
def engine_callback(engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert urlparse(engine_url).path in request.url.path
        return Response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.dict()},
        )

    return do_mock


@fixture
def engine_url(server: str, account_id) -> str:
    return f"https://{server}" + ACCOUNT_LIST_ENGINES_URL.format(account_id=account_id)


@fixture
def account_engine_callback(account_engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == account_engine_url
        return Response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.dict()},
        )

    return do_mock


@fixture
def account_engine_url(server: str, account_id, mock_engine) -> str:
    return f"https://{server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id,
        engine_id=mock_engine.engine_id,
    )


@fixture
def mock_database(region_1: str, account_id: str) -> Database:
    return Database(
        name="database",
        description="mock_db_description",
        compute_region_key=region_1.key,
        database_key=DatabaseKey(
            account_id=account_id, database_id="mock_database_id_1"
        ),
    )


@fixture
def create_databases_callback(databases_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        database_properties = json.loads(request.read().decode("utf-8"))["database"]

        mock_database.name = database_properties["name"]
        mock_database.description = database_properties["description"]

        assert request.url == databases_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


@fixture
def databases_get_callback(databases_url: str, mock_database) -> Callable:
    def get_databases_callback_inner(
        request: httpx.Request = None, **kwargs
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK, json={"edges": [{"node": mock_database.dict()}]}
        )

    return get_databases_callback_inner


@fixture
def databases_url(server: str, account_id: str) -> str:
    return f"https://{server}" + ACCOUNT_DATABASES_URL.format(account_id=account_id)


@fixture
def database_callback(database_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


@fixture
def database_not_found_callback(database_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return Response(
            status_code=httpx.codes.OK,
            json={},
        )

    return do_mock


@fixture
def database_url(server: str, account_id: str, mock_database) -> str:
    return f"https://{server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=mock_database.database_id
    )


@fixture
def database_get_by_name_callback(database_get_by_name_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_by_name_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database_id": {"database_id": mock_database.database_id}},
        )

    return do_mock


@fixture
def database_get_by_name_url(server: str, account_id: str, mock_database) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)
        + f"?database_name={mock_database.name}"
    )


@fixture
def database_update_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        database_properties = json.loads(request.read().decode("utf-8"))["database"]

        assert request.url == database_get_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": database_properties},
        )

    return do_mock


@fixture
def database_get_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


# duplicates database_url
@fixture
def database_get_url(server: str, account_id: str, mock_database) -> str:
    return f"https://{server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=mock_database.database_id
    )
