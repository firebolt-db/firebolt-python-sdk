from datetime import date, datetime
from json import dumps as jdumps
from typing import Any, Callable, Dict, List

from httpx import URL, Request, Response, codes
from pytest import fixture
from pytest_httpx import to_response

from firebolt.async_db import ARRAY, Connection, Cursor, connect
from firebolt.async_db.cursor import JSON_OUTPUT_FORMAT, ColType, Column
from firebolt.common.settings import Settings

QUERY_ROW_COUNT: int = 10


@fixture
def query_description() -> List[Column]:
    return [
        Column("uint8", "UInt8", None, None, None, None, None),
        Column("uint16", "UInt16", None, None, None, None, None),
        Column("uint32", "UInt32", None, None, None, None, None),
        Column("int32", "Int32", None, None, None, None, None),
        Column("uint64", "UInt64", None, None, None, None, None),
        Column("int64", "Int64", None, None, None, None, None),
        Column("float32", "Float32", None, None, None, None, None),
        Column("float64", "Float64", None, None, None, None, None),
        Column("string", "String", None, None, None, None, None),
        Column("date", "Date", None, None, None, None, None),
        Column("datetime", "DateTime", None, None, None, None, None),
        Column("bool", "UInt8", None, None, None, None, None),
        Column("array", "Array(UInt8)", None, None, None, None, None),
    ]


@fixture
def python_query_description() -> List[Column]:
    return [
        Column("uint8", int, None, None, None, None, None),
        Column("uint16", int, None, None, None, None, None),
        Column("uint32", int, None, None, None, None, None),
        Column("int32", int, None, None, None, None, None),
        Column("uint64", int, None, None, None, None, None),
        Column("int64", int, None, None, None, None, None),
        Column("float32", float, None, None, None, None, None),
        Column("float64", float, None, None, None, None, None),
        Column("string", str, None, None, None, None, None),
        Column("date", date, None, None, None, None, None),
        Column("datetime", datetime, None, None, None, None, None),
        Column("bool", int, None, None, None, None, None),
        Column("array", ARRAY(int), None, None, None, None, None),
    ]


@fixture
def query_data() -> List[List[ColType]]:
    return [
        [
            i,
            256,
            70000,
            -32768,
            922337203685477580,
            -922337203685477580,
            1,
            1.0387398573,
            "some text",
            "2019-07-31",
            "2019-07-31 01:01:01",
            1,
            [1, 2, 3, 4],
        ]
        for i in range(QUERY_ROW_COUNT)
    ]


@fixture
def python_query_data() -> List[List[ColType]]:
    return [
        [
            i,
            256,
            70000,
            -32768,
            922337203685477580,
            -922337203685477580,
            1,
            1.0387398573,
            "some text",
            date(2019, 7, 31),
            datetime(2019, 7, 31, 1, 1, 1),
            1,
            [1, 2, 3, 4],
        ]
        for i in range(QUERY_ROW_COUNT)
    ]


@fixture
def query_callback(
    query_description: List[Column], query_data: List[List[ColType]]
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        query_response = {
            "meta": [{"name": c.name, "type": c.type_code} for c in query_description],
            "data": query_data,
            "rows": len(query_data),
            # Real example of statistics field value, not used by our code
            "statistics": {
                "elapsed": 0.002983335,
                "time_before_execution": 0.002729331,
                "time_to_execute": 0.000215215,
                "rows_read": 1,
                "bytes_read": 1,
                "scanned_bytes_cache": 0,
                "scanned_bytes_storage": 0,
            },
        }
        return to_response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def query_with_params_callback(
    query_description: List[Column],
    query_data: List[List[ColType]],
    set_params: Dict,
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        set_parameters = request.url.params
        for k, v in set_params.items():
            assert k in set_parameters and set_parameters[k] == encode_param(
                v
            ), "Invalid set parameters passed"
        query_response = {
            "meta": [{"name": c.name, "type": c.type_code} for c in query_description],
            "data": query_data,
            "rows": len(query_data),
            # Real example of statistics field value, not used by our code
            "statistics": {
                "elapsed": 0.002983335,
                "time_before_execution": 0.002729331,
                "time_to_execute": 0.000215215,
                "rows_read": 1,
                "bytes_read": 1,
                "scanned_bytes_cache": 0,
                "scanned_bytes_storage": 0,
            },
        }
        return to_response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def insert_query_callback(
    query_description: List[Column], query_data: List[List[ColType]]
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        return to_response(status_code=codes.OK, headers={"content-length": "0"})

    return do_query


def encode_param(p: Any) -> str:
    return jdumps(p).strip('"')


@fixture
def set_params() -> Dict:
    return {"param1": 1, "param2": "2", "param3": True}


@fixture
def query_url(settings: Settings, db_name: str) -> str:
    return URL(
        f"https://{settings.server}?database={db_name}"
        f"&output_format={JSON_OUTPUT_FORMAT}"
    )


@fixture
def query_with_params_url(query_url: str, set_params: str) -> str:
    params_encoded = "&".join([f"{k}={encode_param(v)}" for k, v in set_params.items()])
    query_url = f"{query_url}&{params_encoded}"


@fixture
async def connection(settings: Settings, db_name: str) -> Connection:
    async with (
        await connect(
            engine_url=settings.server,
            database=db_name,
            username="u",
            password="p",
            api_endpoint=settings.server,
        )
    ) as connection:
        yield connection


@fixture
async def cursor(connection: Connection, settings: Settings) -> Cursor:
    return connection.cursor()


@fixture
def types_map() -> Dict[str, type]:
    base_types = {
        "UInt8": int,
        "UInt16": int,
        "UInt32": int,
        "Int32": int,
        "UInt64": int,
        "Int64": int,
        "Float32": float,
        "Float64": float,
        "String": str,
        "Date": date,
        "DateTime": datetime,
        "Nullable(Nothing)": str,
        "SomeRandomNotExistingType": str,
    }
    array_types = {f"Array({k})": ARRAY(v) for k, v in base_types.items()}
    nullable_arrays = {f"Nullable({k})": v for k, v in array_types.items()}
    nested_arrays = {f"Array({k})": ARRAY(v) for k, v in array_types.items()}
    return {**base_types, **array_types, **nullable_arrays, **nested_arrays}
