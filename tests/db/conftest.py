from datetime import date, datetime
from typing import Callable, List

from httpx import URL, Request, Response, codes
from pytest import fixture
from pytest_httpx import to_response

from firebolt.client import FireboltClient
from firebolt.common.settings import Settings
from firebolt.db import Cursor
from firebolt.db.cursor import JSON_OUTPUT_FORMAT, ColType, Column
from firebolt.db.typing import ARRAY

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
def query_url(settings: Settings, db_name: str) -> str:
    return URL(
        f"https://{settings.server}?database={db_name}"
        f"&output_format={JSON_OUTPUT_FORMAT}"
    )


# TODO: Propper connection after it will be implemented
@fixture
def connection(db_name: str):
    class connection:
        database = db_name

    return connection()


@fixture
def cursor(connection, settings: Settings) -> Cursor:
    return Cursor(
        FireboltClient(
            auth=("u", "p"),
            base_url=f"https://{settings.server}",
            api_endpoint=settings.server,
        ),
        connection,
    )
