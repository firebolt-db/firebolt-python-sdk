import json
import re
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from json import dumps as jdumps
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import parse_qs

from httpx import URL, Request, codes
from pytest import fixture
from pytest_httpx import HTTPXMock

from firebolt.async_db.cursor import ColType
from firebolt.common._types import STRUCT
from firebolt.common.constants import (
    JSON_LINES_OUTPUT_FORMAT,
    JSON_OUTPUT_FORMAT,
    REMOVE_PARAMETERS_HEADER,
    RESET_SESSION_HEADER,
    TRANSACTION_ID_SETTING,
    TRANSACTION_SEQUENCE_ID_SETTING,
    UPDATE_ENDPOINT_HEADER,
    UPDATE_PARAMETERS_HEADER,
)
from firebolt.common.row_set.json_lines import (
    DataRecord,
    ErrorRecord,
    MessageType,
    StartRecord,
    SuccessRecord,
)
from firebolt.common.row_set.types import Column
from firebolt.db import ARRAY, DECIMAL
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME
from tests.unit.response import Response

QUERY_ROW_COUNT: int = 10


@fixture
def query_description() -> List[Column]:
    return [
        Column("uint8", "int", None, None, None, None, None),
        Column("uint16", "int", None, None, None, None, None),
        Column("uint32", "int", None, None, None, None, None),
        Column("int32", "int", None, None, None, None, None),
        Column("uint64", "long", None, None, None, None, None),
        Column("int64", "long", None, None, None, None, None),
        Column("float32", "float", None, None, None, None, None),
        Column("float64", "double", None, None, None, None, None),
        Column("string", "text", None, None, None, None, None),
        Column("date", "date", None, None, None, None, None),
        Column("date32", "date_ext", None, None, None, None, None),
        Column("datetime", "timestamp", None, None, None, None, None),
        Column("datetime64", "timestamp_ext", None, None, None, None, None),
        Column("bool", "boolean", None, None, None, None, None),
        Column("array", "array(int)", None, None, None, None, None),
        Column("decimal", "Decimal(12, 34)", None, None, None, None, None),
        Column("bytea", "bytea", None, None, None, None, None),
        Column("geography", "geography", None, None, None, None, None),
    ]


@fixture
def streaming_result_columns(query_description) -> List[Dict[str, str]]:
    def map_type(t: str) -> str:
        alternatives = {
            "int": "integer",
            "long": "bigint",
            "double": "double precision",
        }
        return alternatives.get(t, t)

    return [
        {"name": col.name, "type": map_type(col.type_code)} for col in query_description
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
        Column("date32", date, None, None, None, None, None),
        Column("datetime", datetime, None, None, None, None, None),
        Column("datetime64", datetime, None, None, None, None, None),
        Column("bool", bool, None, None, None, None, None),
        Column("array", ARRAY(int), None, None, None, None, None),
        Column("decimal", DECIMAL(12, 34), None, None, None, None, None),
        Column("bytea", bytes, None, None, None, None, None),
        Column("geography", str, None, None, None, None, None),
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
            "1.0387398573",
            "some text",
            "2019-07-31",
            "1860-01-31",
            "2019-07-31 01:01:01",
            "2020-07-31 01:01:01.1234",
            1,
            [1, 2, 3, 4],
            "123456789.123456789123456789123456789",
            "\\x616263",
            "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F",
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
            date(1860, 1, 31),
            datetime(2019, 7, 31, 1, 1, 1),
            datetime(2020, 7, 31, 1, 1, 1, 123400),
            1,
            [1, 2, 3, 4],
            Decimal("123456789.123456789123456789123456789"),
            b"abc",
            "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F",
        ]
        for i in range(QUERY_ROW_COUNT)
    ]


@fixture
def query_statistics() -> Dict[str, Any]:
    # Just some dummy statistics to have in query response
    return {
        "elapsed": 0.116907717,
        "rows_read": 1,
        "bytes_read": 61,
        "time_before_execution": 0.012180623,
        "time_to_execute": 0.104614307,
        "scanned_bytes_cache": 0,
        "scanned_bytes_storage": 0,
    }


@fixture
def query_callback(
    query_description: List[Column],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_OUTPUT_FORMAT}" in str(request.url)
        query_response = {
            "meta": [{"name": c.name, "type": c.type_code} for c in query_description],
            "data": query_data,
            "rows": len(query_data),
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def query_callback_with_headers(
    query_description: List[Column],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
    db_name_updated: str,
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_OUTPUT_FORMAT}" in str(request.url)
        query_response = {
            "meta": [{"name": c.name, "type": c.type_code} for c in query_description],
            "data": query_data,
            "rows": len(query_data),
            "statistics": query_statistics,
        }
        headers = {UPDATE_PARAMETERS_HEADER: f"database={db_name_updated}"}
        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


@fixture
def select_one_query_callback(
    query_description: List[Column],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        query_response = {
            "meta": [{"name": "select 1", "type": "Int8"}],
            "data": [{"select 1": 1}],
            "rows": 1,
            # Real example of statistics field value, not used by our code
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def query_with_params_callback(
    query_description: List[Column],
    query_data: List[List[ColType]],
    set_params: Dict,
    query_statistics: Dict[str, Any],
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
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def insert_query_callback(
    query_description: List[Column], query_data: List[List[ColType]]
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        return Response(status_code=codes.OK, headers={"content-length": "0"})

    return do_query


@fixture
def remove_parameters() -> List[str]:
    return ["param1", "param3"]


@fixture
def query_callback_with_remove_header(
    query_statistics: Dict[str, Any], remove_parameters: List[str]
) -> Callable:
    """Fixture for query callback that returns REMOVE_PARAMETERS_HEADER.

    Returns a callback that simulates a server response with Firebolt-Remove-Parameters
    header containing 'param1,param3' to test parameter removal functionality.
    """

    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        query_response = {
            "meta": [{"name": "one", "type": "int"}],
            "data": [1],
            "rows": 1,
            "statistics": query_statistics,
        }
        # Header with comma-separated parameter names to remove
        headers = {REMOVE_PARAMETERS_HEADER: ",".join(remove_parameters)}
        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


def encode_param(p: Any) -> str:
    return jdumps(p).strip('"')


@fixture
def set_params() -> Dict:
    return {"param1": 1, "param2": "2", "param3": 1}


@fixture
def query_url(engine_url: str, db_name: str) -> URL:
    return URL(
        f"https://{engine_url}/",
        params={"output_format": JSON_OUTPUT_FORMAT, "database": db_name},
    )


@fixture
def async_query_url(engine_url: str, db_name: str) -> URL:
    return URL(
        f"https://{engine_url}/",
        params={
            "output_format": JSON_OUTPUT_FORMAT,
            "database": db_name,
            "async": "true",
        },
    )


@fixture
def streaming_query_url(engine_url: str, db_name: str) -> URL:
    return URL(
        f"https://{engine_url}/",
        params={
            "output_format": JSON_LINES_OUTPUT_FORMAT,
            "database": db_name,
        },
    )


@fixture
def query_url_updated(engine_url: str, db_name_updated: str) -> URL:
    return URL(
        f"https://{engine_url}/",
        params={"output_format": JSON_OUTPUT_FORMAT, "database": db_name_updated},
    )


@fixture
def set_query_url(engine_url: str, db_name: str) -> URL:
    return URL(f"https://{engine_url}/?database={db_name}")


@fixture
def system_engine_url() -> str:
    return "https://bravo.a.eu-west-1.aws.mock.firebolt.io"


@fixture
def system_engine_query_url(system_engine_url: str, db_name: str) -> str:
    return f"{system_engine_url}/?output_format=JSON_Compact&database={db_name}"


@fixture
def system_engine_no_db_query_url(system_engine_url: str) -> str:
    return f"{system_engine_url}/?output_format=JSON_Compact"


@fixture
def get_system_engine_url(api_endpoint: str, account_name: str) -> URL:
    return URL(
        f"https://{api_endpoint}"
        f"{GATEWAY_HOST_BY_ACCOUNT_NAME.format(account_name=account_name)}"
    )


@fixture
def get_system_engine_callback(system_engine_url: str) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "GET", "invalid request method"

        return Response(
            status_code=codes.OK,
            json={"engineUrl": system_engine_url},
        )

    return inner


@fixture
def get_system_engine_404_callback() -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request.method == "GET", "invalid request method"

        return Response(
            status_code=codes.NOT_FOUND,
            json={"error": "not found"},
        )

    return inner


@fixture
def use_database_callback(db_name: str, query_statistics: Dict[str, Any]) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "POST", "invalid request method"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        return Response(
            status_code=codes.OK,
            json=query_response,
            headers={UPDATE_PARAMETERS_HEADER: f"database={db_name}"},
        )

    return inner


@fixture
def use_database_failed_callback(
    db_name: str, query_statistics: Dict[str, Any]
) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "POST", "invalid request method"

        return Response(
            status_code=codes.INTERNAL_SERVER_ERROR,
            content="use database failed",
        )

    return inner


@fixture
def use_engine_callback(engine_url: str, query_statistics: Dict[str, Any]) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "POST", "invalid request method"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        return Response(
            status_code=codes.OK,
            json=query_response,
            headers={UPDATE_ENDPOINT_HEADER: engine_url},
        )

    return inner


@fixture
def test_update_parameters() -> Dict[str, str]:
    """Test parameters used by use_engine_with_params_callback."""
    return {
        "custom_param": "test_value",
        "another_param": "123",
    }


@fixture
def use_engine_with_params_callback(
    engine_url: str,
    query_statistics: Dict[str, Any],
    test_update_parameters: Dict[str, str],
) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "POST", "invalid request method"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        # Return both endpoint update and parameter update headers
        param_string = ",".join([f"{k}={v}" for k, v in test_update_parameters.items()])
        headers = {
            UPDATE_ENDPOINT_HEADER: engine_url,
            UPDATE_PARAMETERS_HEADER: param_string,
        }

        return Response(
            status_code=codes.OK,
            json=query_response,
            headers=headers,
        )

    return inner


@fixture
def use_engine_failed_callback(
    engine_url, db_name, query_statistics: Dict[str, Any]
) -> Callable:
    def inner(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        assert request.method == "POST", "invalid request method"

        return Response(
            status_code=codes.INTERNAL_SERVER_ERROR,
            content="Use engine failed",
        )

    return inner


@fixture
def mock_system_engine_connection_flow(
    httpx_mock: HTTPXMock,
    auth_url: str,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
) -> Callable:
    def inner() -> None:
        httpx_mock.add_callback(
            check_credentials_callback, url=auth_url, is_reusable=True
        )
        httpx_mock.add_callback(
            get_system_engine_callback, url=get_system_engine_url, is_reusable=True
        )

    return inner


@fixture
def mock_connection_flow(
    httpx_mock: HTTPXMock,
    mock_system_engine_connection_flow: Callable,
    engine_name: str,
    db_name: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    system_engine_query_url: str,
) -> Callable:
    def inner() -> None:
        mock_system_engine_connection_flow()

        httpx_mock.add_callback(
            use_database_callback,
            url=system_engine_no_db_query_url,
            match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
            is_reusable=True,
        )
        httpx_mock.add_callback(
            use_engine_callback,
            url=system_engine_query_url,
            match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
            is_reusable=True,
        )

    return inner


@fixture
def mock_query(
    httpx_mock: HTTPXMock,
    query_url: str,
    query_callback: Callable,
) -> Callable:
    def inner() -> None:
        httpx_mock.add_callback(query_callback, url=query_url, is_reusable=True)

    return inner


@fixture
def mock_insert_query(
    httpx_mock: HTTPXMock,
    query_url: str,
    insert_query_callback: Callable,
) -> Callable:
    def inner() -> None:
        httpx_mock.add_callback(insert_query_callback, url=query_url, is_reusable=True)

    return inner


@fixture
def types_map() -> Dict[str, type]:
    base_types = {
        "int": int,
        "integer": int,
        "long": int,
        "bigint": int,
        "float": float,
        "double": float,
        "double precision": float,
        "text": str,
        "date": date,
        "pgdate": date,
        "timestamp": datetime,
        "timestampntz": datetime,
        "timestamptz": datetime,
        "Nothing": str,
        "Decimal(123, 4)": DECIMAL(123, 4),
        "Decimal(38,0)": DECIMAL(38, 0),
        "numeric(123, 4)": DECIMAL(123, 4),
        "numeric(38,0)": DECIMAL(38, 0),
        # Invalid decimal format
        "Decimal(38)": str,
        "boolean": bool,
        "SomeRandomNotExistingType": str,
        "bytea": bytes,
    }
    nullable_types = {f"{k} null": v for k, v in base_types.items()}
    array_types = {
        f"array({k})": ARRAY(v)
        for k, v in (*base_types.items(), *nullable_types.items())
    }
    nullable_arrays = {f"{k} null": v for k, v in array_types.items()}
    nested_arrays = {f"array({k})": ARRAY(v) for k, v in array_types.items()}

    struct_keys, struct_fields = list(
        zip(*base_types.items(), *nullable_types.items(), *array_types.items())
    )
    # Create column names by replacing invalid characters with underscores
    trans = str.maketrans({ch: "_" for ch in " (),"})

    struct_items = [f"{key.translate(trans)}_col {key}" for key in struct_keys]
    struct_type = f"struct({', '.join(struct_items)})"
    struct_field_names = [f"{key.translate(trans)}_col" for key in struct_keys]
    struct = {struct_type: STRUCT(dict(zip(struct_field_names, struct_fields)))}
    nested_struct = {
        f"struct(s {struct_type} null)": STRUCT({"s": list(struct.values())[0]})
    }

    return {
        **base_types,
        **nullable_types,
        **array_types,
        **nullable_arrays,
        **nested_arrays,
        **struct,
        **nested_struct,
    }


@fixture
def async_query_data() -> List[List[ColType]]:
    query_data = [
        [
            "developer",
            "ecosystem_ci",
            "2025-01-23 14:08:06.087953+00",
            "2025-01-23 14:08:06.134208+00",
            "2025-01-23 14:08:06.410542+00",
            "ENDED_SUCCESSFULLY",
            "db4c7542-3058-4e2a-9d49-ae5ea2da3cbe",
            "f9520387-224c-48e9-9858-b2d05518ce94",
            "",
            "2",
            "2",
            "0",
        ]
    ]
    return query_data


@fixture
def async_multiple_query_data() -> List[List[ColType]]:
    query_data = [
        [
            "developer",
            "ecosystem_ci",
            "2025-01-23 14:08:06.087953+00",
            "2025-01-23 14:08:06.134208+00",
            "2025-01-23 14:08:06.410542+00",
            "RUNNING",
            "",
            "987e6543-e21b-34d3-b654-426614174111",
            "",
            "2",
            "2",
            "0",
        ],
        [
            "developer",
            "ecosystem_ci",
            "2025-01-23 14:08:06.087953+00",
            "2025-01-23 14:08:06.134208+00",
            "2025-01-23 14:08:06.410542+00",
            "ENDED_SUCCESSFULLY",
            "123e4567-e89b-12d3-a456-426614174000",
            "f9520387-224c-48e9-9858-b2d05518ce94",
            "",
            "2",
            "2",
            "0",
        ],
    ]
    return query_data


@fixture
def async_query_meta() -> List[Tuple[str, str]]:
    query_meta = [
        ("account_name", "text null"),
        ("user_name", "text null"),
        ("submitted_time", "timestamptz null"),
        ("start_time", "timestamptz null"),
        ("end_time", "timestamptz null"),
        ("status", "text null"),
        ("request_id", "text null"),
        ("query_id", "text null"),
        ("error_message", "text null"),
        ("scanned_bytes", "long null"),
        ("scanned_rows", "long null"),
        ("retries", "long null"),
    ]
    return query_meta


@fixture
def async_query_callback_factory(
    query_statistics: Dict[str, Any],
) -> Callable:
    def create_callback(
        data: List[List[ColType]], meta: List[Tuple[str, str]]
    ) -> Callable:
        def do_query(request: Request, **kwargs) -> Response:
            assert request.read() != b""
            assert request.method == "POST"
            assert f"output_format={JSON_OUTPUT_FORMAT}" in str(request.url)
            query_response = {
                "meta": [{"name": c[0], "type": c[1]} for c in meta],
                "data": data,
                "rows": len(data),
                "statistics": query_statistics,
            }
            return Response(status_code=codes.OK, json=query_response)

        return do_query

    return create_callback


@fixture
def async_query_status_running_callback(
    query_statistics: Dict[str, Any],
    query_data: List[List[ColType]],
    query_meta: List[Tuple[str, str]],
) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_OUTPUT_FORMAT}" in str(request.url)
        query_response = {
            "meta": [{"name": c[0], "type": c[1]} for c in query_meta],
            "data": query_data,
            "rows": len(query_data),
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def async_token() -> str:
    return "async_token"


@fixture
def async_query_callback(async_token: str) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_OUTPUT_FORMAT}" in str(request.url)
        assert "async=true" in str(request.url)
        query_response = {
            "message": "the query was accepted for async processing",
            "monitorSql": "CALL fb_GetAsyncStatus('token');",
            "token": async_token,
        }
        return Response(status_code=codes.ACCEPTED, json=query_response)

    return do_query


@fixture
def streaming_query_response(
    streaming_result_columns: List[Dict[str, str]],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
) -> str:
    records = [
        StartRecord(
            message_type=MessageType.start.value,
            result_columns=streaming_result_columns,
            query_id="query_id",
            query_label="query_label",
            request_id="request_id",
        ),
        DataRecord(message_type=MessageType.data.value, data=query_data),
        SuccessRecord(
            message_type=MessageType.success.value, statistics=query_statistics
        ),
    ]
    return "\n".join(json.dumps(asdict(record)) for record in records)


@fixture
def streaming_insert_query_response(
    query_statistics: Dict[str, Any],
) -> str:
    records = [
        StartRecord(
            message_type=MessageType.start.value,
            result_columns=[],
            query_id="query_id",
            query_label="query_label",
            request_id="request_id",
        ),
        SuccessRecord(
            message_type=MessageType.success.value, statistics=query_statistics
        ),
    ]
    return "\n".join(json.dumps(asdict(record)) for record in records)


@fixture
def streaming_query_callback(streaming_query_response) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_LINES_OUTPUT_FORMAT}" in str(request.url)
        return Response(status_code=codes.OK, content=streaming_query_response)

    return do_query


@fixture
def streaming_insert_query_callback(streaming_insert_query_response) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_LINES_OUTPUT_FORMAT}" in str(request.url)
        return Response(status_code=codes.OK, content=streaming_insert_query_response)

    return do_query


@fixture
def streaming_error_query_response(
    streaming_result_columns: List[Dict[str, str]],
    query_statistics: Dict[str, Any],
) -> str:
    error_message = "Query execution error: Table 'large_table' doesn't exist"
    records = [
        StartRecord(
            message_type=MessageType.start.value,
            result_columns=streaming_result_columns,
            query_id="query_id",
            query_label="query_label",
            request_id="request_id",
        ),
        ErrorRecord(
            message_type=MessageType.error.value,
            errors=[{"message": error_message}],
            query_id="error_query_id",
            query_label="error_query_label",
            request_id="error_request_id",
            statistics=query_statistics,
        ),
    ]
    return "\n".join(json.dumps(asdict(record)) for record in records)


@fixture
def streaming_error_query_callback(streaming_error_query_response) -> Callable:
    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        assert f"output_format={JSON_LINES_OUTPUT_FORMAT}" in str(request.url)
        return Response(status_code=codes.OK, content=streaming_error_query_response)

    return do_query


@fixture
def fb_numeric_test_parameters() -> List[List[Any]]:
    """Test parameters covering all supported types for fb_numeric paramstyle."""
    return [
        # Basic types
        [42, "string", 3.14, True, False, None],
        # Edge cases
        [0, "", 0.0, -1, -3.14],
        # Complex types that should be converted to strings
        [datetime(2023, 1, 1), date(2023, 1, 1)],
        # Single parameter
        [1],
        # Empty parameters
        [],
    ]


@fixture
def fb_numeric_expected_query_params() -> List[List[Dict[str, Any]]]:
    """Expected query_parameters JSON for corresponding test parameters."""
    return [
        # Basic types
        [
            {"name": "$1", "value": 42},
            {"name": "$2", "value": "string"},
            {"name": "$3", "value": 3.14},
            {"name": "$4", "value": True},
            {"name": "$5", "value": False},
            {"name": "$6", "value": None},
        ],
        # Edge cases
        [
            {"name": "$1", "value": 0},
            {"name": "$2", "value": ""},
            {"name": "$3", "value": 0.0},
            {"name": "$4", "value": -1},
            {"name": "$5", "value": -3.14},
        ],
        # Complex types (converted to strings in sync, kept as-is in async)
        [
            {"name": "$1", "value": "2023-01-01 00:00:00"},  # sync behavior
            {"name": "$2", "value": "2023-01-01"},  # sync behavior
        ],
        # Single parameter
        [
            {"name": "$1", "value": 1},
        ],
        # Empty parameters
        [],
    ]


@fixture
def fb_numeric_query_url(engine_url: str, db_name: str) -> re.Pattern:
    """Regex pattern for fb_numeric queries that matches base URL regardless of query parameters."""
    base_url = f"https://{engine_url}"
    return re.compile(rf"^{re.escape(base_url)}.*")


@fixture
def fb_numeric_query_url_exact(engine_url: str, db_name: str) -> URL:
    """Exact URL for fb_numeric queries."""
    return URL(f"https://{engine_url}").copy_merge_params(
        {"database": db_name, "output_format": "JSON_Compact"}
    )


@fixture
def fb_numeric_callback_factory(
    query_description: List[Column],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
) -> Callable:
    """Factory for creating fb_numeric query callbacks that validate parameters."""

    def create_callback(
        expected_query_params: List[Dict[str, Any]],
        expected_query: str = None,
        is_async: bool = False,
    ) -> Callable:
        def do_query(request: Request, **kwargs) -> Response:
            assert request.method == "POST"

            # Validate query parameters in URL
            qs = parse_qs(request.url.query)
            query_params_raw = qs.get(b"query_parameters", [])

            if query_params_raw:
                query_params_str = query_params_raw[0]
                if isinstance(query_params_str, bytes):
                    query_params_str = query_params_str.decode()
                actual_query_params = json.loads(query_params_str)
                assert actual_query_params == expected_query_params, (
                    f"Expected query_parameters: {expected_query_params}, "
                    f"got: {actual_query_params}"
                )
            else:
                assert (
                    expected_query_params == []
                ), f"Expected empty query_parameters, but URL has: {dict(qs)}"

            # Validate query content if provided
            if expected_query:
                body = request.content.decode() if request.content else ""
                assert expected_query in body or expected_query == body

            # Return appropriate response
            if is_async:
                return Response(
                    status_code=codes.OK,
                    json={
                        "query_id": "test-async-token-123",
                        "statistics": query_statistics,
                    },
                )
            else:
                query_response = {
                    "meta": [
                        {"name": col.name, "type": col.type_code}
                        for col in query_description
                    ],
                    "data": query_data,
                    "rows": len(query_data),
                    "statistics": query_statistics,
                }
                return Response(status_code=codes.OK, json=query_response)

        return do_query

    return create_callback


@fixture
def fb_numeric_simple_callback(
    query_statistics: Dict[str, Any],
) -> Callable:
    """Simple callback for fb_numeric queries that just returns success."""

    def do_query(request: Request, **kwargs) -> Response:
        assert request.method == "POST"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }
        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def fb_numeric_async_callback(async_token: str) -> Callable:
    """Callback for fb_numeric async queries."""

    def do_query(request: Request, **kwargs) -> Response:
        assert request.method == "POST"

        # Validate async=True parameter
        qs = parse_qs(request.url.query)
        async_param = qs.get(b"async", [])
        assert async_param == [b"true"], f"Expected async=true, got: {async_param}"

        return Response(
            status_code=codes.OK,
            json={
                "token": async_token,
                "message": "Query submitted successfully",
                "monitorSql": "SELECT 1",
            },
        )

    return do_query


# Transaction fixtures
@fixture
def transaction_id() -> str:
    return "test_transaction_id_12345"


@fixture
def transaction_sequence_id() -> int:
    return 1


@fixture
def begin_transaction_callback(
    transaction_id: str, query_statistics: Dict[str, Any]
) -> Callable:
    """Mock callback for BEGIN transaction that returns transaction_id."""

    def do_query(request: Request, **kwargs) -> Response:
        request_body = request.read().decode("utf-8")
        assert "BEGIN" in request_body.upper()
        assert request.method == "POST"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        headers = {
            UPDATE_PARAMETERS_HEADER: f"{TRANSACTION_ID_SETTING}={transaction_id}"
        }

        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


@fixture
def transaction_query_callback(
    transaction_id: str,
    transaction_sequence_id: int,
    query_description: List[Column],
    query_data: List[List[ColType]],
    query_statistics: Dict[str, Any],
) -> Callable:
    """Mock callback for queries within transaction that returns sequence_id."""

    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"

        # Check that transaction parameters are passed
        url_params = dict(request.url.params)
        assert (
            TRANSACTION_ID_SETTING in url_params
        ), f"Expected {TRANSACTION_ID_SETTING} in params, got: {url_params}"
        assert (
            url_params[TRANSACTION_ID_SETTING] == transaction_id
        ), f"Expected {TRANSACTION_ID_SETTING}={transaction_id}, got: {url_params[TRANSACTION_ID_SETTING]}"

        query_response = {
            "meta": [{"name": c.name, "type": c.type_code} for c in query_description],
            "data": query_data,
            "rows": len(query_data),
            "statistics": query_statistics,
        }

        # Return incremented sequence id
        headers = {
            UPDATE_PARAMETERS_HEADER: f"{TRANSACTION_SEQUENCE_ID_SETTING}={transaction_sequence_id + 1}"
        }

        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


@fixture
def commit_transaction_callback(
    transaction_id: str,
    transaction_sequence_id: int,
    query_statistics: Dict[str, Any],
) -> Callable:
    """Mock callback for COMMIT transaction that resets transaction state."""

    def do_query(request: Request, **kwargs) -> Response:
        request_body = request.read().decode("utf-8")
        assert "COMMIT" in request_body.upper()
        assert request.method == "POST"

        # Check that transaction parameters are passed
        url_params = dict(request.url.params)
        assert (
            TRANSACTION_ID_SETTING in url_params
        ), f"Expected {TRANSACTION_ID_SETTING} in params, got: {url_params}"
        assert (
            url_params[TRANSACTION_ID_SETTING] == transaction_id
        ), f"Expected {TRANSACTION_ID_SETTING}={transaction_id}, got: {url_params[TRANSACTION_ID_SETTING]}"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        # Reset session header to clear transaction state
        headers = {RESET_SESSION_HEADER: "true"}

        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


@fixture
def simple_commit_callback(query_statistics: Dict[str, Any]) -> Callable:
    """Mock callback for COMMIT transaction when not in transaction."""

    def do_query(request: Request, **kwargs) -> Response:
        request_body = request.read().decode("utf-8")
        assert "COMMIT" in request_body.upper()
        assert request.method == "POST"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        return Response(status_code=codes.OK, json=query_response)

    return do_query


@fixture
def rollback_transaction_callback(
    transaction_id: str,
    transaction_sequence_id: int,
    query_statistics: Dict[str, Any],
) -> Callable:
    """Mock callback for ROLLBACK transaction that resets transaction state."""

    def do_query(request: Request, **kwargs) -> Response:
        request_body = request.read().decode("utf-8")
        assert "ROLLBACK" in request_body.upper()
        assert request.method == "POST"

        # Check that transaction parameters are passed
        url_params = dict(request.url.params)
        assert (
            TRANSACTION_ID_SETTING in url_params
        ), f"Expected {TRANSACTION_ID_SETTING} in params, got: {url_params}"
        assert (
            url_params[TRANSACTION_ID_SETTING] == transaction_id
        ), f"Expected {TRANSACTION_ID_SETTING}={transaction_id}, got: {url_params[TRANSACTION_ID_SETTING]}"

        query_response = {
            "meta": [],
            "data": [],
            "rows": 0,
            "statistics": query_statistics,
        }

        # Reset session header to clear transaction state
        headers = {RESET_SESSION_HEADER: "true"}

        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query


@fixture
def transaction_with_remove_params_callback(
    query_statistics: Dict[str, Any],
) -> Callable:
    """Mock callback that returns REMOVE_PARAMETERS_HEADER for transaction params."""

    def do_query(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"

        query_response = {
            "meta": [{"name": "result", "type": "int"}],
            "data": [1],
            "rows": 1,
            "statistics": query_statistics,
        }

        # Header to remove transaction parameters
        headers = {
            REMOVE_PARAMETERS_HEADER: f"{TRANSACTION_ID_SETTING},{TRANSACTION_SEQUENCE_ID_SETTING}"
        }

        return Response(status_code=codes.OK, json=query_response, headers=headers)

    return do_query
