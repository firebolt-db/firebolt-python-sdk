from dataclasses import dataclass, fields
from datetime import datetime
from typing import Callable, List

import httpx
from pytest import fixture

from firebolt.client.auth import Auth
from firebolt.common._types import _InternalType
from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine
from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.manager import ResourceManager
from firebolt.service.V2.types import EngineStatus
from tests.unit.response import Response


@fixture
def mock_engine(
    engine_name: str, api_endpoint: str, instance_type: InstanceType
) -> Engine:
    return Engine(
        name=engine_name,
        region="",
        spec=instance_type,
        scale=2,
        current_status=EngineStatus.STOPPED,
        version="",
        endpoint=api_endpoint,
        warmup="",
        auto_stop=7200,
        type="",
        _database_name="database",
        _service=None,
    )


@fixture
def mock_engine_stopping(
    engine_name: str, api_endpoint: str, instance_type: InstanceType
) -> Engine:
    return Engine(
        name=engine_name,
        region="",
        spec=instance_type,
        scale=2,
        current_status=EngineStatus.STOPPING,
        version="",
        endpoint=api_endpoint,
        warmup="",
        auto_stop=7200,
        type="",
        _database_name="database",
        _service=None,
    )


@fixture
def instance_type() -> InstanceType:
    return InstanceType.M


@fixture
def mock_database(db_name: str) -> Database:
    return Database(
        name=db_name,
        description="mock_db_description",
        region="",
        data_size_full=0,
        data_size_compressed=0,
        create_time=datetime.now().isoformat(),
        create_actor="",
        _attached_engine_names="-",
        _errors="",
        _service=None,
    )


@fixture
def mock_database_2() -> Database:
    return Database(
        name="database2",
        description="completely different db",
        region="",
        data_size_full=0,
        data_size_compressed=0,
        create_time=datetime.now().isoformat(),
        create_actor="",
        _attached_engine_names="-",
        _errors="",
        _service=None,
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
            "Union[str, InstanceType]": _InternalType.Text.value,
            "EngineStatus": _InternalType.Text.value,
        }
        dc_fields = [f for f in fields(objs[0]) if f.name != "_service"]

        def get_obj_field(obj, f):
            value = getattr(obj, f.name)
            if isinstance(value, (InstanceType, EngineStatus)):
                return value.value
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
def get_engine_callback_stopping(mock_engine_stopping: Engine) -> Callable:
    return get_objects_from_db_callback([mock_engine_stopping])


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
def updated_auto_stop() -> int:
    return 0


@fixture
def update_engine_callback(
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
    updated_engine_scale: int,
    updated_auto_stop: int,
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == system_engine_no_db_query_url
        mock_engine.scale = updated_engine_scale
        mock_engine.auto_stop = updated_auto_stop
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
    api_endpoint: str,
    mock_system_engine_connection_flow: Callable,
) -> ResourceManager:
    mock_system_engine_connection_flow()
    return ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )
