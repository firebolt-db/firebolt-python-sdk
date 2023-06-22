from dataclasses import dataclass, fields
from datetime import datetime
from typing import Callable, List

import httpx
from httpx import Response
from pytest import fixture

from firebolt.client.auth import Auth
from firebolt.common._types import _InternalType
from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager
from firebolt.service.types import EngineStatus, EngineType, WarmupMethod
from firebolt.utils.urls import ACCOUNT_INSTANCE_TYPES_URL
from tests.unit.util import list_to_paginated_response


@fixture
def region() -> str:
    return "us-east-1"


@fixture
def mock_engine(region: str, server: str, instance_type_1: InstanceType) -> Engine:
    return Engine(
        name="engine_1",
        region=region,
        spec=instance_type_1,
        scale=2,
        current_status=EngineStatus.STOPPED,
        version="",
        endpoint=server,
        warmup=WarmupMethod.MINIMAL,
        auto_stop=7200,
        type=EngineType.GENERAL_PURPOSE,
        provisioning="Finished",
        _database_name="database",
        _service=None,
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
def mock_database(region: str) -> Database:
    return Database(
        name="database",
        description="mock_db_description",
        region=region,
        data_size_full=0,
        data_size_compressed=0,
        create_time=datetime.now().isoformat(),
        create_actor="",
        _status="",
        _attached_engine_names="-",
        _errors="",
        _service=None,
    )


@fixture
def mock_database_2(region: str) -> Database:
    return Database(
        name="database2",
        description="completely different db",
        region=region,
        data_size_full=0,
        data_size_compressed=0,
        create_time=datetime.now().isoformat(),
        create_actor="",
        _status="",
        _attached_engine_names="-",
        _errors="",
        _service=None,
    )


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


empty_response = {
    "meta": [],
    "data": [],
    "rows": 0,
    "statistics": {
        "elapsed": 39635.785423446,
        "rows_read": 0,
        "bytes_read": 0,
        "time_before_execution": 0,
        "time_to_execute": 0,
    },
}


def get_objects_from_db_callback(objs: List[dataclass]) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        fieldname = lambda f: (f.metadata or {}).get("db_name", f.name)
        types = {
            "int": _InternalType.Long.value,
            "str": _InternalType.Text.value,
            "datetime": _InternalType.Text.value,  # we receive datetime as text from db
            "InstanceType": _InternalType.Text.value,
            "EngineType": _InternalType.Text.value,
            "EngineStatus": _InternalType.Text.value,
        }
        dc_fields = [f for f in fields(objs[0]) if f.name != "_service"]

        def get_obj_field(obj, f):
            value = getattr(obj, f.name)
            if isinstance(value, (InstanceType, EngineStatus)):
                return str(value)
            if isinstance(value, (EngineType, WarmupMethod)):
                return " ".join(
                    map(lambda s: s.capitalize(), str(value).lower().split("_"))
                )
            return value

        query_response = {
            "meta": [{"name": fieldname(f), "type": types[f.type]} for f in dc_fields],
            "data": [[get_obj_field(obj, f) for f in dc_fields] for obj in objs],
            "rows": len(objs),
            "statistics": {
                "elapsed": 0.116907717,
                "rows_read": 1,
                "bytes_read": 61,
                "time_before_execution": 0.012180623,
                "time_to_execute": 0.104614307,
                "scanned_bytes_cache": 0,
                "scanned_bytes_storage": 0,
            },
        }
        return Response(
            status_code=httpx.codes.OK,
            json=query_response,
        )

    return do_mock


@fixture
def get_engine_callback(mock_engine: Engine) -> Callable:
    return get_objects_from_db_callback([mock_engine])


@fixture
def get_engine_not_found_callback(mock_engine: Engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def attach_engine_to_db_callback(system_engine_no_db_query_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == system_engine_no_db_query_url
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def updated_engine_scale() -> int:
    return 10


@fixture
def update_engine_callback(
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
    updated_engine_scale: int,
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == system_engine_no_db_query_url
        mock_engine.scale = updated_engine_scale
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def create_databases_callback(
    system_engine_no_db_query_url: str, mock_database
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == system_engine_no_db_query_url
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def databases_get_callback(
    mock_database: Database, mock_database_2: Database
) -> Callable:
    return get_objects_from_db_callback([mock_database, mock_database_2])


@fixture
def database_not_found_callback() -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def database_update_callback() -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json=empty_response,
        )

    return do_mock


@fixture
def database_get_callback(mock_database) -> Callable:
    return get_objects_from_db_callback([mock_database])


@fixture
def resource_manager(
    auth: Auth,
    account_name: str,
    server: str,
    mock_system_engine_connection_flow: Callable,
) -> ResourceManager:
    mock_system_engine_connection_flow()
    return ResourceManager(auth=auth, account_name=account_name, api_endpoint=server)
