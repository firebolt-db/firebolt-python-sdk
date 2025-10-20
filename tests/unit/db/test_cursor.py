import json
import re
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List
from unittest.mock import patch
from urllib.parse import parse_qs

from httpx import URL, HTTPStatusError, Request, StreamError, codes
from pytest import LogCaptureFixture, mark, raises
from pytest_httpx import HTTPXMock

from firebolt.common.constants import CursorState
from firebolt.common.row_set.types import Column
from firebolt.db import Cursor
from firebolt.db.cursor import ColType, ProgrammingError
from firebolt.utils.exception import (
    ConfigurationError,
    CursorClosedError,
    DataError,
    FireboltError,
    FireboltStructuredError,
    MethodNotAllowedInAsyncError,
    OperationalError,
    ProgrammingError,
    QueryNotRunError,
    QueryTimeoutError,
)
from tests.unit.db_conftest import encode_param
from tests.unit.response import Response


def test_cursor_state(
    httpx_mock: HTTPXMock,
    mock_query: Callable,
    query_url: str,
    cursor: Cursor,
):
    """Cursor state changes depending on the operations performed with it."""
    mock_query()

    assert cursor._state == CursorState.NONE

    cursor.execute("select")
    assert cursor._state == CursorState.DONE

    def error_query_callback(*args, **kwargs):
        raise Exception()

    httpx_mock.add_callback(error_query_callback, url=query_url, is_reusable=True)

    cursor._reset()
    with raises(Exception):
        cursor.execute("select")
    assert cursor._state == CursorState.ERROR

    cursor._reset()
    assert cursor._state == CursorState.NONE

    cursor.close()
    assert cursor._state == CursorState.CLOSED


def test_closed_cursor(cursor: Cursor):
    """Most of cursor methods are unavailable for closed cursor."""
    fields = ("description", "rowcount")
    methods = (
        ("execute", (cursor)),
        ("executemany", (cursor, [])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
        ("setinputsizes", (cursor, [0])),
        ("setoutputsize", (cursor, 0)),
        ("nextset", ()),
    )

    cursor.close()

    for field in fields:
        with raises(CursorClosedError):
            getattr(cursor, field)

    for method, args in methods:
        with raises(CursorClosedError):
            print(method, args)
            getattr(cursor, method)(*args)

    with raises(CursorClosedError):
        with cursor:
            pass

    with raises(CursorClosedError):
        list(cursor)

    # No errors
    assert cursor.closed
    cursor.close()


def test_cursor_no_query(
    httpx_mock: HTTPXMock,
    mock_query: Callable,
    cursor: Cursor,
):
    """Some of cursor methods are unavailable until a query is run."""
    methods = (
        "fetchone",
        "fetchmany",
        "fetchall",
        "nextset",
    )

    mock_query()

    for method in methods:
        with raises(QueryNotRunError):
            getattr(cursor, method)()

    with raises(QueryNotRunError):
        list(cursor)

    # No errors
    assert not cursor.description
    cursor._reset()
    assert cursor.rowcount == -1
    cursor._reset()
    assert not cursor.closed
    cursor._reset()
    cursor.execute("select")
    cursor._reset()
    cursor.executemany("select", [])
    cursor._reset()
    cursor.setinputsizes([0])
    cursor._reset()
    cursor.setoutputsize(0)

    # Context manager is also available
    with cursor:
        pass


def test_cursor_execute(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
    python_query_description: List[Column],
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    for query, message in (
        (
            lambda: cursor.execute("select * from t"),
            "server-side synchronous execute()",
        ),
        (
            lambda: cursor.executemany("select * from t", []),
            "server-side synchronous executemany()",
        ),
    ):
        # Query with json output
        mock_query()
        assert query() == len(
            python_query_data
        ), f"Invalid row count returned for {message}."
        assert cursor.rowcount == len(
            python_query_data
        ), f"Invalid rowcount value for {message}."
        for i, (desc, exp) in enumerate(
            zip(cursor.description, python_query_description)
        ):
            assert (
                desc == exp
            ), f"Invalid column description at position {i} for {message}."

        for i in range(cursor.rowcount):
            assert (
                cursor.fetchone() == python_query_data[i]
            ), f"Invalid data row at position {i} for {message}."

        assert (
            cursor.fetchone() is None
        ), f"Non-empty fetchone after all data received for {message}."

        # Query with empty output
        mock_insert_query()
        assert query() == -1, f"Invalid row count for insert using {message}."
        assert (
            cursor.rowcount == -1
        ), f"Invalid rowcount value for insert using {message}."
        assert (
            cursor.description == []
        ), f"Invalid description for insert using {message}."


def test_cursor_execute_error(
    httpx_mock: HTTPXMock,
    env_name,
    db_name: str,
    query_url: str,
    query_statistics: Dict[str, Any],
    cursor: Cursor,
    system_engine_query_url: str,
):
    """Cursor handles all types of errors properly."""
    for query, message in (
        (
            lambda: cursor.execute("select * from t"),
            "server-side synchronous execute()",
        ),
        (
            lambda: cursor.executemany("select * from t", []),
            "server-side synchronous executemany()",
        ),
    ):
        # Internal httpx error
        def http_error(*args, **kwargs):
            raise StreamError("httpx error")

        httpx_mock.add_callback(http_error, url=query_url, is_reusable=True)
        with raises(StreamError) as excinfo:
            query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "httpx error"
        ), f"Invalid query error message for {message}."

        # HTTP error
        httpx_mock.add_callback(
            lambda *args, **kwargs: Response(
                status_code=codes.BAD_REQUEST,
            ),
            url=query_url,
        )
        with raises(HTTPStatusError) as excinfo:
            query()

        errmsg = str(excinfo.value)
        assert cursor._state == CursorState.ERROR
        assert "Bad Request" in errmsg, f"Invalid query error message for {message}."

        # Database query error
        httpx_mock.add_callback(
            lambda *args, **kwargs: Response(
                status_code=codes.INTERNAL_SERVER_ERROR,
                content="Query error message",
            ),
            url=query_url,
        )
        with raises(OperationalError) as excinfo:
            query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "Error executing query:\nQuery error message"
        ), f"Invalid authentication error message for {message}."

        # Database exists but some other error
        error_message = "My query error message"
        httpx_mock.add_callback(
            lambda *args, **kwargs: Response(
                status_code=codes.FORBIDDEN,
                content=error_message,
            ),
            url=query_url,
            match_content=b"select * from t",
        )
        with raises(ProgrammingError) as excinfo:
            query()
        assert cursor._state == CursorState.ERROR
        assert error_message in str(excinfo)

        httpx_mock.reset()


def test_cursor_fetchone(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """cursor fetchone fetches single row in correct order; if no rows returns None."""
    mock_query()

    cursor.execute("sql")

    assert cursor.fetchone()[0] == 0, "Invalid rows order returned by fetchone"
    assert cursor.fetchone()[0] == 1, "Invalid rows order returned by fetchone"

    assert (
        len(cursor.fetchall()) == cursor.rowcount - 2
    ), "Invalid row number returned by fetchall"

    assert (
        cursor.fetchone() is None
    ), "fetchone should return None when no rows left to fetch"

    mock_insert_query()
    cursor.execute("sql")
    with raises(DataError):
        cursor.fetchone()


def test_cursor_fetchmany(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """
    Cursor's fetchmany fetches the provided amount of rows, or arraysize by
    default. If not enough rows left, returns less or None if there are no rows.
    """
    mock_query()

    cursor.execute("sql")

    with raises(TypeError) as excinfo:
        cursor.arraysize = "123"

    assert (
        len(cursor.fetchmany(0)) == 0
    ), "Invalid count of rows returned by fetchmany for 0 size"

    assert (
        str(excinfo.value) == "Invalid arraysize value type, expected int, got str"
    ), "Invalid value error message"
    cursor.arraysize = 2

    many = cursor.fetchmany()
    assert len(many) == cursor.arraysize, "Invalid count of rows returned by fetchmany"
    assert [r[0] for r in many] == [0, 1], "Invalid rows order returned by fetchmany"

    many = cursor.fetchmany(cursor.arraysize + 3)
    assert (
        len(many) == cursor.arraysize + 3
    ), "Invalid count of rows returned by fetchmany with size provided"
    assert [r[0] for r in many] == list(
        range(2, 7)
    ), "Invalid rows order returned by fetchmany"

    # only 3 left at this point
    many = cursor.fetchmany(4)
    assert (
        len(many) == 3
    ), "Invalid count of rows returned by fetchmany for last elements"
    assert [r[0] for r in many] == list(
        range(7, 10)
    ), "Invalid rows order returned by fetchmany"

    assert (
        len(cursor.fetchmany()) == 0
    ), "fetchmany should return empty result set when no rows left to fetch"

    mock_insert_query()
    cursor.execute("sql")
    with raises(DataError):
        cursor.fetchmany()


def test_cursor_fetchall(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """cursor fetchall fetches all rows that left after last query."""
    mock_query()

    cursor.execute("sql")

    cursor.fetchmany(4)
    tail = cursor.fetchall()
    assert (
        len(tail) == cursor.rowcount - 4
    ), "Invalid count of rows returned by fetchall"
    assert [r[0] for r in tail] == list(
        range(4, cursor.rowcount)
    ), "Invalid rows order returned by fetchall"

    assert (
        len(cursor.fetchall()) == 0
    ), "fetchmany should return empty result set when no rows left to fetch"

    mock_insert_query()
    cursor.execute("sql")
    with raises(DataError):
        cursor.fetchall()


def test_cursor_multi_statement(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
    python_query_description: List[Column],
    python_query_data: List[List[ColType]],
):
    """executemany with multiple parameter sets is not supported."""
    mock_query()
    mock_insert_query()
    mock_query()

    rc = cursor.execute("select * from t; insert into t values (1, 2); select * from t")
    assert rc == len(python_query_data), "Invalid row count returned"
    assert cursor.rowcount == len(python_query_data), "Invalid cursor row count"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    for i in range(cursor.rowcount):
        assert (
            cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i}"

    assert cursor.nextset()
    assert cursor.rowcount == -1, "Invalid cursor row count"
    assert cursor.description == [], "Invalid cursor description"
    with raises(DataError) as exc_info:
        cursor.fetchall()

    assert str(exc_info.value) == "no rows to fetch", "Invalid error message"

    assert cursor.nextset()

    assert cursor.rowcount == len(python_query_data), "Invalid cursor row count"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    for i in range(cursor.rowcount):
        assert (
            cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i}"

    assert cursor.nextset() is False


def test_cursor_set_statements(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    set_query_url: str,
    cursor: Cursor,
):
    """cursor correctly parses and processes set statements."""
    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&a=b",
        is_reusable=True,
    )

    assert len(cursor._set_parameters) == 0

    rc = cursor.execute("set a = b")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "a" in cursor._set_parameters
        and cursor._set_parameters["a"] == "b"
    )

    cursor.flush_parameters()

    assert len(cursor._set_parameters) == 0

    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&param1=1",
        is_reusable=True,
    )

    rc = cursor.execute("set param1=1")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "param1" in cursor._set_parameters
        and cursor._set_parameters["param1"] == "1"
    )

    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&param1=1&param2=0",
        is_reusable=True,
    )

    rc = cursor.execute("set param2=0")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        cursor.fetchall()

    assert len(cursor._set_parameters) == 2

    assert (
        "param1" in cursor._set_parameters and cursor._set_parameters["param1"] == "1"
    )

    assert (
        "param2" in cursor._set_parameters and cursor._set_parameters["param2"] == "0"
    )

    cursor.flush_parameters()

    assert len(cursor._set_parameters) == 0


def test_cursor_set_parameters_sent(
    httpx_mock: HTTPXMock,
    set_query_url: str,
    query_url: str,
    query_with_params_callback: Callable,
    select_one_query_callback: Callable,
    cursor: Cursor,
    set_params: Dict,
):
    """Cursor passes provided set parameters to engine."""
    params = ""

    for p, v in set_params.items():
        v = encode_param(v)
        params += f"&{p}={v}"
        httpx_mock.add_callback(
            select_one_query_callback,
            url=f"{set_query_url}{params}",
            is_reusable=True,
        )
        cursor.execute(f"set {p} = {v}")

    httpx_mock.add_callback(
        query_with_params_callback,
        url=f"{query_url}{params}",
        is_reusable=True,
    )
    cursor.execute("select 1")


def test_cursor_skip_parse(
    mock_query: Callable,
    cursor: Cursor,
):
    """Cursor doesn't process a query if skip_parsing is provided."""
    mock_query()

    with patch(
        "firebolt.common.statement_formatter.StatementFormatter.split_format_sql",
        return_value=["sql"],
    ) as split_format_sql_mock:
        cursor.execute("non-an-actual-sql")
        split_format_sql_mock.assert_called_once()

    with patch(
        "firebolt.common.statement_formatter.StatementFormatter.split_format_sql"
    ) as split_format_sql_mock:
        cursor.execute("non-an-actual-sql", skip_parsing=True)
        split_format_sql_mock.assert_not_called()


def test_cursor_iterate(
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    httpx_mock.add_callback(query_callback, url=query_url, is_reusable=True)

    with raises(QueryNotRunError):
        for res in cursor:
            pass

    cursor.execute("select * from t")
    i = 0
    for res in cursor:
        assert res == python_query_data[i]
        i += 1
    assert i == len(python_query_data), "Wrong number iterations of a cursor were done"

    cursor.close()
    with raises(CursorClosedError):
        for res in cursor:
            pass


def test_server_side_header_database(
    httpx_mock: HTTPXMock,
    query_callback_with_headers: Callable,
    query_url: str,
    query_url_updated: str,
    db_name: str,
    db_name_updated: str,
    cursor: Cursor,
):
    httpx_mock.add_callback(
        query_callback_with_headers, url=query_url, is_reusable=True
    )
    assert cursor.database == db_name
    cursor.execute(f"USE DATABASE = '{db_name_updated}'")
    assert cursor.database == db_name_updated

    httpx_mock.reset()
    # Check updated database is used in the next query
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=query_url_updated,
        is_reusable=True,
    )
    cursor.execute("select 1")
    assert cursor.database == db_name_updated


def test_cursor_unknown_error_body_logging(
    httpx_mock: HTTPXMock, cursor: Cursor, caplog: LogCaptureFixture, query_url: str
):
    actual_error_body = "Your query was incorrect"
    httpx_mock.add_callback(
        lambda *args, **kwargs: Response(
            status_code=codes.NOT_IMPLEMENTED, content=actual_error_body
        ),
        url=query_url,
    )
    with raises(HTTPStatusError):
        cursor.execute("select 1")
    assert actual_error_body in caplog.text


@mark.parametrize(
    "parameter",
    [
        "database",
        "engine",
        "output_format",
    ],
)
def test_disallowed_set_parameter(cursor: Cursor, parameter: str) -> None:
    """Test that setting disallowed parameters raises an error."""
    with raises(ConfigurationError) as e:
        cursor.execute(f"SET {parameter}=dummy")
    assert f"Set parameter '{parameter}' is not allowed" in str(
        e.value
    ), "invalid error"
    assert cursor._set_parameters == {}, "set parameters should not be updated"


def test_cursor_use_engine_no_parameters(
    httpx_mock: HTTPXMock,
    query_url: URL,
    cursor: Cursor,
    query_statistics: Dict[str, Any],
):
    query_updated_url = "my_dummy_url"

    def query_callback_with_headers(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        query_response = {
            "meta": [{"name": "one", "type": "int"}],
            "data": [1],
            "rows": 1,
            "statistics": query_statistics,
        }
        headers = {"Firebolt-Update-Endpoint": f"https://{query_updated_url}"}
        return Response(status_code=codes.OK, json=query_response, headers=headers)

    httpx_mock.add_callback(
        query_callback_with_headers, url=query_url, is_reusable=True
    )
    assert cursor.engine_url == "https://" + query_url.host
    cursor.execute("USE ENGINE = 'my_dummy_engine'")
    assert cursor.engine_url == f"https://{query_updated_url}"

    httpx_mock.reset()
    # Check updated engine is used in the next query
    new_url = query_url.copy_with(host=query_updated_url)
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=new_url,
        is_reusable=True,
    )
    cursor.execute("select 1")
    assert cursor.engine_url == f"https://{query_updated_url}"


def test_cursor_use_engine_with_parameters(
    httpx_mock: HTTPXMock,
    query_url: URL,
    cursor: Cursor,
    query_statistics: Dict[str, Any],
):
    query_updated_url = "my_dummy_url"
    param_string_dummy = "param1=1&param2=2&engine=my_dummy_engine"

    header = {
        "Firebolt-Update-Endpoint": f"https://{query_updated_url}/?{param_string_dummy}"
    }

    def query_callback_with_headers(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        query_response = {
            "meta": [{"name": "one", "type": "int"}],
            "data": [1],
            "rows": 1,
            "statistics": query_statistics,
        }
        headers = header
        return Response(status_code=codes.OK, json=query_response, headers=headers)

    httpx_mock.add_callback(
        query_callback_with_headers, url=query_url, is_reusable=True
    )
    assert cursor.engine_url == "https://" + query_url.host
    cursor.execute("USE ENGINE = 'my_dummy_engine'")
    assert cursor.engine_url == f"https://{query_updated_url}"
    assert cursor._set_parameters == {"param1": "1", "param2": "2"}
    assert list(cursor.parameters.keys()) == ["database", "engine"]
    assert cursor.engine_name == "my_dummy_engine"

    httpx_mock.reset()
    # Check new parameters are used in the URL
    new_url = query_url.copy_with(host=query_updated_url).copy_merge_params(
        {"param1": "1", "param2": "2", "engine": "my_dummy_engine"}
    )
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=new_url,
        is_reusable=True,
    )
    cursor.execute("select 1")
    assert cursor.engine_url == f"https://{query_updated_url}"


def test_cursor_reset_session(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    set_query_url: str,
    cursor: Cursor,
    query_statistics: Dict[str, Any],
):
    def query_callback_with_headers(request: Request, **kwargs) -> Response:
        assert request.read() != b""
        assert request.method == "POST"
        query_response = {
            "meta": [{"name": "one", "type": "int"}],
            "data": [1],
            "rows": 1,
            "statistics": query_statistics,
        }
        headers = {"Firebolt-Reset-Session": "any_value_here"}
        return Response(status_code=codes.OK, json=query_response, headers=headers)

    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&a=b",
        is_reusable=True,
    )

    assert len(cursor._set_parameters) == 0

    cursor.execute("set a = b")
    assert (
        len(cursor._set_parameters) == 1
        and "a" in cursor._set_parameters
        and cursor._set_parameters["a"] == "b"
    )

    httpx_mock.reset()
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=f"{set_query_url}&a=b&output_format=JSON_Compact",
        is_reusable=True,
    )
    cursor.execute("SELECT 1")
    assert len(cursor._set_parameters) == 0
    assert bool(cursor.engine_url) is True, "engine url is not set"
    assert bool(cursor.database) is True, "database is not set"


def test_cursor_remove_parameters_header(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    query_callback_with_remove_header: Callable,
    set_query_url: str,
    cursor: Cursor,
):
    """Test that cursor removes parameters when REMOVE_PARAMETERS_HEADER is received."""

    # Set up initial parameters
    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&param1=value1",
        is_reusable=True,
    )
    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&param1=value1&param2=value2",
        is_reusable=True,
    )
    httpx_mock.add_callback(
        select_one_query_callback,
        url=f"{set_query_url}&param1=value1&param2=value2&param3=value3",
        is_reusable=True,
    )

    assert len(cursor._set_parameters) == 0

    # Execute SET statements to add parameters
    cursor.execute("set param1 = value1")
    cursor.execute("set param2 = value2")
    cursor.execute("set param3 = value3")

    assert len(cursor._set_parameters) == 3
    assert "param1" in cursor._set_parameters
    assert "param2" in cursor._set_parameters
    assert "param3" in cursor._set_parameters
    assert cursor._set_parameters["param1"] == "value1"
    assert cursor._set_parameters["param2"] == "value2"
    assert cursor._set_parameters["param3"] == "value3"

    # Execute query that returns remove parameters header
    httpx_mock.reset()
    httpx_mock.add_callback(
        query_callback_with_remove_header,
        url=f"{set_query_url}&param1=value1&param2=value2&param3=value3&output_format=JSON_Compact",
        is_reusable=True,
    )
    cursor.execute("SELECT 1")

    # Verify that param1 and param3 were removed, param2 remains
    assert len(cursor._set_parameters) == 1
    assert "param1" not in cursor._set_parameters
    assert "param2" in cursor._set_parameters
    assert "param3" not in cursor._set_parameters
    assert cursor._set_parameters["param2"] == "value2"


def test_cursor_timeout(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    cursor: Cursor,
):
    fast_executed, long_executed = False, False

    def fast_query_callback(request: Request, **kwargs) -> Response:
        nonlocal fast_executed
        fast_executed = True
        return select_one_query_callback(request, **kwargs)

    def long_query_callback(request: Request, **kwargs) -> Response:
        nonlocal long_executed
        time.sleep(2)
        long_executed = True
        return select_one_query_callback(request, **kwargs)

    httpx_mock.add_callback(long_query_callback)
    httpx_mock.add_callback(fast_query_callback)

    with raises(QueryTimeoutError):
        cursor.execute("SELECT 1; SELECT 2", timeout_seconds=1)

    assert long_executed is True, "long query was executed"
    assert fast_executed is False, "fast query was not executed"

    httpx_mock.reset()


def verify_async_fetch_not_allowed(cursor: Cursor):
    with raises(MethodNotAllowedInAsyncError):
        cursor.fetchall()
    with raises(MethodNotAllowedInAsyncError):
        cursor.fetchone()
    with raises(MethodNotAllowedInAsyncError):
        cursor.fetchmany()


def test_cursor_execute_async(
    httpx_mock: HTTPXMock,
    async_query_callback: Callable,
    async_query_url: str,
    cursor: Cursor,
    async_token: str,
):
    httpx_mock.add_callback(
        async_query_callback,
        url=async_query_url,
        is_reusable=True,
    )
    cursor.execute_async("SELECT 2")
    verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


def test_cursor_execute_async_multiple_queries(
    cursor: Cursor,
):
    with raises(FireboltError) as e:
        cursor.execute_async("SELECT 2; SELECT 3")
    assert "does not support multi-statement" in str(e.value)


def test_cursor_execute_async_parametrised_query(
    httpx_mock: HTTPXMock,
    async_query_callback: Callable,
    async_query_url: str,
    cursor: Cursor,
    async_token: str,
):
    httpx_mock.add_callback(
        async_query_callback,
        url=async_query_url,
        is_reusable=True,
    )
    cursor.execute_async("SELECT 2 WHERE x = ?", [1])
    verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


def test_cursor_execute_async_skip_parsing(
    httpx_mock: HTTPXMock,
    async_query_callback: Callable,
    async_query_url: str,
    cursor: Cursor,
    async_token: str,
):
    httpx_mock.add_callback(
        async_query_callback,
        url=async_query_url,
        is_reusable=True,
    )
    cursor.execute_async("SELECT 2; SELECT 3", skip_parsing=True)
    verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


def test_cursor_execute_async_validate_set_parameters(
    cursor: Cursor,
):
    with raises(FireboltError) as e:
        cursor.execute_async("SET a = b")
    assert "does not support set" in str(e.value)


def test_cursor_execute_async_respects_api_errors(
    httpx_mock: HTTPXMock,
    async_query_url: str,
    cursor: Cursor,
):
    httpx_mock.add_callback(
        lambda *args, **kwargs: Response(status_code=codes.BAD_REQUEST),
        url=async_query_url,
    )
    with raises(HTTPStatusError):
        cursor.execute_async("SELECT 2")


def test_cursor_execute_stream(
    httpx_mock: HTTPXMock,
    streaming_query_url: str,
    streaming_query_callback: Callable,
    streaming_insert_query_callback: Callable,
    cursor: Cursor,
    python_query_description: List[Column],
    python_query_data: List[List[ColType]],
):
    httpx_mock.add_callback(
        streaming_query_callback,
        url=streaming_query_url,
        is_reusable=True,
    )
    cursor.execute_stream("select * from large_table")
    assert (
        cursor.rowcount == -1
    ), f"Expected row count to be -1 until the end of streaming for execution with streaming"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    for i in range(len(python_query_data)):
        assert (
            cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i} for execution with streaming."

    assert (
        cursor.fetchone() is None
    ), f"Non-empty fetchone after all data received for execution with streaming."

    assert cursor.rowcount == len(
        python_query_data
    ), f"Invalid rowcount value after streaming finished for execute with streaming."

    # Query with empty output
    httpx_mock.add_callback(
        streaming_insert_query_callback,
        url=streaming_query_url,
        is_reusable=True,
    )
    cursor.execute_stream("insert into t values (1, 2)")
    assert (
        cursor.rowcount == -1
    ), f"Invalid rowcount value for insert using execution with streaming."
    assert (
        cursor.description == []
    ), f"Invalid description for insert using execution with streaming."
    assert (
        cursor.fetchone() is None
    ), f"Invalid statistics for insert using execution with streaming."


def test_cursor_execute_stream_error(
    httpx_mock: HTTPXMock,
    streaming_query_url: str,
    cursor: Cursor,
    streaming_error_query_callback: Callable,
):
    """Test error handling in execute_stream method."""

    # Test HTTP error (connection error)
    def http_error(*args, **kwargs):
        raise StreamError("httpx streaming error")

    httpx_mock.add_callback(
        http_error,
        url=streaming_query_url,
        is_reusable=True,
    )
    with raises(StreamError) as excinfo:
        cursor.execute_stream("select * from large_table")

    assert cursor._state == CursorState.ERROR
    assert str(excinfo.value) == "httpx streaming error"

    httpx_mock.reset()

    # Test HTTP status error
    httpx_mock.add_callback(
        lambda *args, **kwargs: Response(
            status_code=codes.BAD_REQUEST,
        ),
        url=streaming_query_url,
    )
    with raises(HTTPStatusError) as excinfo:
        cursor.execute_stream("select * from large_table")

    assert cursor._state == CursorState.ERROR
    assert "Bad Request" in str(excinfo.value)

    httpx_mock.reset()

    # Test in-body error (ErrorRecord)
    httpx_mock.add_callback(
        streaming_error_query_callback,
        url=streaming_query_url,
        is_reusable=True,
    )

    for method in (cursor.fetchone, cursor.fetchmany, cursor.fetchall):
        # Execution works fine
        cursor.execute_stream("select * from large_table")

        # Error is raised during streaming
        with raises(FireboltStructuredError):
            method()


@mark.parametrize(
    "test_params,expected_query_params",
    [
        # Basic types
        (
            [42, "string", 3.14, True, False, None],
            [
                {"name": "$1", "value": 42},
                {"name": "$2", "value": "string"},
                {"name": "$3", "value": 3.14},
                {"name": "$4", "value": True},
                {"name": "$5", "value": False},
                {"name": "$6", "value": None},
            ],
        ),
        # Edge cases for numeric types
        (
            [0, -1, 0.0, -3.14],
            [
                {"name": "$1", "value": 0},
                {"name": "$2", "value": -1},
                {"name": "$3", "value": 0.0},
                {"name": "$4", "value": -3.14},
            ],
        ),
        # String edge cases
        (
            ["", "multi\nline", "special'chars\"test"],
            [
                {"name": "$1", "value": ""},
                {"name": "$2", "value": "multi\nline"},
                {"name": "$3", "value": "special'chars\"test"},
            ],
        ),
        # Single parameter
        ([42], [{"name": "$1", "value": 42}]),
        # Empty parameters
        ([], []),
    ],
)
def test_fb_numeric_parameter_formatting(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    test_params: List[Any],
    expected_query_params: List[Dict[str, Any]],
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle formats parameters correctly for various types."""
    test_query = f"SELECT * FROM test WHERE col IN ({', '.join(f'${i+1}' for i in range(len(test_params)))})"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    cursor.execute(test_query, test_params)


def test_fb_numeric_complex_types_converted_to_strings(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle converts complex types to strings in sync mode."""
    test_params = [datetime(2023, 1, 1, 12, 30), date(2023, 6, 15)]
    expected_query_params = [
        {"name": "$1", "value": "2023-01-01 12:30:00"},
        {"name": "$2", "value": "2023-06-15"},
    ]

    test_query = "SELECT * FROM test WHERE created_at = $1 AND birth_date = $2"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    cursor.execute(test_query, test_params)


def test_fb_numeric_no_client_side_substitution(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
):
    """Test that fb_numeric paramstyle does not perform client-side parameter substitution."""
    test_query = "SELECT * FROM test WHERE id = $1 AND name = $2 AND value = $1"
    test_params = [42, "test"]
    expected_query_params = [
        {"name": "$1", "value": 42},
        {"name": "$2", "value": "test"},
    ]

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"
        cursor.execute(test_query, test_params)
    finally:
        db.paramstyle = original_paramstyle


def test_fb_numeric_executemany(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_simple_callback: Callable,
):
    """Test that fb_numeric paramstyle works with executemany method."""
    test_query = "INSERT INTO test (id, name) VALUES ($1, $2)"

    # For executemany, only the first parameter set is used with fb_numeric
    test_params_seq = [
        [1, "first"],
        [2, "second"],
        [3, "third"],
    ]

    def validate_executemany_callback(request: Request, **kwargs) -> Response:
        assert request.method == "POST"

        # Should process multiple parameter sets sequentially
        qs = parse_qs(request.url.query)
        query_params_raw = qs.get(b"query_parameters", [])

        if query_params_raw:
            query_params_str = query_params_raw[0].decode()
            actual_query_params = json.loads(query_params_str)
            # Should have parameters from one of the parameter sets
            assert len(actual_query_params) == 2
            assert actual_query_params[0]["name"] == "$1"
            assert actual_query_params[1]["name"] == "$2"

        return fb_numeric_simple_callback(request, **kwargs)

    httpx_mock.add_callback(
        validate_executemany_callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"
        cursor.executemany(test_query, test_params_seq)
    finally:
        db.paramstyle = original_paramstyle


def test_fb_numeric_with_cursor_set_parameters(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_simple_callback: Callable,
):
    """Test that fb_numeric paramstyle works correctly when cursor has pre-existing set parameters."""
    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"

        # Manually set a parameter in the cursor (simulating what would happen
        # if SET was called in a different paramstyle mode)
        cursor._set_parameters = {"my_param": "test_value"}

        test_query = "SELECT * FROM test WHERE id = $1"
        test_params = [42]

        def validate_with_set_params_callback(request: Request, **kwargs):
            assert request.method == "POST"

            # Should include both set parameters and query parameters
            qs = parse_qs(request.url.query)

            # Check for set parameter
            assert b"my_param" in qs
            assert qs[b"my_param"] == [b"test_value"]

            # Check for query parameters
            query_params_raw = qs.get(b"query_parameters", [])
            if query_params_raw:
                query_params_str = query_params_raw[0].decode()
                actual_query_params = json.loads(query_params_str)
                expected = [{"name": "$1", "value": 42}]
                assert actual_query_params == expected

            return fb_numeric_simple_callback(request, **kwargs)

        # Mock the SELECT query with set parameters
        httpx_mock.add_callback(
            validate_with_set_params_callback,
            url=fb_numeric_query_url,
            is_reusable=True,
        )

        cursor.execute(test_query, test_params)
    finally:
        db.paramstyle = original_paramstyle


@mark.parametrize(
    "test_params,expected_query_params",
    [
        # Decimal types
        (
            [Decimal("123.45"), Decimal("0"), Decimal("-999.999")],
            [
                {"name": "$1", "value": "123.45"},
                {"name": "$2", "value": "0"},
                {"name": "$3", "value": "-999.999"},
            ],
        ),
        # Bytes values
        (
            [b"hello", b"\x00\x01\x02", b""],
            [
                {"name": "$1", "value": "hello"},
                {"name": "$2", "value": "\x00\x01\x02"},
                {"name": "$3", "value": ""},
            ],
        ),
        # List/Array values
        (
            [[1, 2, 3], ["a", "b"], [], [None, True, False]],
            [
                {"name": "$1", "value": [1, 2, 3]},
                {"name": "$2", "value": ["a", "b"]},
                {"name": "$3", "value": []},
                {"name": "$4", "value": [None, True, False]},
            ],
        ),
        # Mixed complex types
        (
            [Decimal("42.0"), b"binary", [1, "mixed"], {"key": "value"}],
            [
                {"name": "$1", "value": "42.0"},
                {"name": "$2", "value": "binary"},
                {"name": "$3", "value": [1, "mixed"]},
                {"name": "$4", "value": "{'key': 'value'}"},
            ],
        ),
        # Large numbers and edge cases
        (
            [2**63 - 1, -(2**63), float("inf"), float("-inf")],
            [
                {"name": "$1", "value": 9223372036854775807},
                {"name": "$2", "value": -9223372036854775808},
                {"name": "$3", "value": float("inf")},
                {"name": "$4", "value": float("-inf")},
            ],
        ),
    ],
)
def test_fb_numeric_additional_types(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    test_params: List[Any],
    expected_query_params: List[Dict[str, Any]],
):
    """Test that fb_numeric paramstyle handles additional SDK-supported types correctly."""
    test_query = f"SELECT * FROM test WHERE col IN ({', '.join(f'${i+1}' for i in range(len(test_params)))})"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"
        cursor.execute(test_query, test_params)
    finally:
        db.paramstyle = original_paramstyle


def test_fb_numeric_nested_complex_structures(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
):
    """Test that fb_numeric paramstyle handles deeply nested structures."""
    test_params = [
        {"nested": {"array": [1, 2, {"deep": True}]}},
        # Here decimal is a part of the JSON so it should be converted to string
        [{"mixed": Decimal("123.45")}, [1, [2, [3]]]],
    ]
    expected_query_params = [
        {"name": "$1", "value": "{'nested': {'array': [1, 2, {'deep': True}]}}"},
        {"name": "$2", "value": ["{'mixed': Decimal('123.45')}", [1, [2, [3]]]]},
    ]

    test_query = "SELECT * FROM test WHERE data = $1 AND metadata = $2"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"
        cursor.execute(test_query, test_params)
    finally:
        db.paramstyle = original_paramstyle


def test_fb_numeric_large_parameter_count(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
):
    """Test that fb_numeric paramstyle handles a large number of parameters."""
    # Test with 50 parameters
    test_params = list(range(50))
    expected_query_params = [{"name": f"${i+1}", "value": i} for i in range(50)]

    placeholders = ", ".join(f"${i+1}" for i in range(50))
    test_query = f"SELECT * FROM test WHERE id IN ({placeholders})"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "fb_numeric"
        cursor.execute(test_query, test_params)
    finally:
        db.paramstyle = original_paramstyle


def test_unsupported_paramstyle_raises(cursor):
    """Test that unsupported paramstyles raise ProgrammingError."""
    import firebolt.db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "not_a_style"
        with raises(ProgrammingError):
            cursor.execute("SELECT 1")
    finally:
        db.paramstyle = original_paramstyle


def test_executemany_bulk_insert_qmark_works(
    httpx_mock: HTTPXMock,
    cursor: Cursor,
    query_url: str,
):
    """executemany with bulk_insert=True works with qmark paramstyle."""

    def bulk_insert_callback(request):
        query = request.content.decode()
        # Should contain multiple INSERT statements
        assert query.count("INSERT INTO") == 3
        assert "; " in query

        return Response(
            status_code=200,
            content=json.dumps(
                {
                    "meta": [],
                    "data": [],
                    "rows": 0,
                    "statistics": {
                        "elapsed": 0.0,
                        "rows_read": 0,
                        "bytes_read": 0,
                    },
                }
            ),
            headers={},
        )

    base_url = str(query_url).split("?")[0]
    url_pattern = re.compile(re.escape(base_url))
    httpx_mock.add_callback(bulk_insert_callback, url=url_pattern)

    result = cursor.executemany(
        "INSERT INTO test_table VALUES (?, ?)",
        [(1, "a"), (2, "b"), (3, "c")],
        bulk_insert=True,
    )
    assert result == 0


def test_executemany_bulk_insert_fb_numeric(
    httpx_mock: HTTPXMock,
    cursor: Cursor,
    query_url: str,
):
    """executemany with bulk_insert=True and FB_NUMERIC style."""
    import firebolt.db as db_module

    original_paramstyle = db_module.paramstyle

    try:
        db_module.paramstyle = "fb_numeric"

        def bulk_insert_callback(request):
            query = request.content.decode()
            assert query.count("INSERT INTO") == 3
            assert "; " in query

            query_params = json.loads(request.url.params.get("query_parameters", "[]"))
            assert len(query_params) == 6
            assert query_params[0]["name"] == "$1"
            assert query_params[5]["name"] == "$6"

            return Response(
                status_code=200,
                content=json.dumps(
                    {
                        "meta": [],
                        "data": [],
                        "rows": 0,
                        "statistics": {
                            "elapsed": 0.0,
                            "rows_read": 0,
                            "bytes_read": 0,
                        },
                    }
                ),
                headers={},
            )

        base_url = str(query_url).split("?")[0]
        url_pattern = re.compile(re.escape(base_url))
        httpx_mock.add_callback(bulk_insert_callback, url=url_pattern)

        result = cursor.executemany(
            "INSERT INTO test_table VALUES ($1, $2)",
            [(1, "a"), (2, "b"), (3, "c")],
            bulk_insert=True,
        )
        assert result == 0
    finally:
        db_module.paramstyle = original_paramstyle


def test_executemany_bulk_insert_non_insert_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails for non-INSERT queries."""
    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        cursor.executemany(
            "SELECT * FROM test_table",
            [()],
            bulk_insert=True,
        )

    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        cursor.executemany(
            "UPDATE test_table SET col = $1",
            [(1,)],
            bulk_insert=True,
        )

    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        cursor.executemany(
            "DELETE FROM test_table WHERE id = $1",
            [(1,)],
            bulk_insert=True,
        )


def test_executemany_bulk_insert_multi_statement_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails for multi-statement queries."""
    with raises(
        ProgrammingError, match="bulk_insert does not support multi-statement queries"
    ):
        cursor.executemany(
            "INSERT INTO test_table VALUES ($1); SELECT * FROM test_table",
            [(1,)],
            bulk_insert=True,
        )

    with raises(
        ProgrammingError, match="bulk_insert does not support multi-statement queries"
    ):
        cursor.executemany(
            "INSERT INTO test_table VALUES ($1); INSERT INTO test_table VALUES ($2)",
            [(1,), (2,)],
            bulk_insert=True,
        )


def test_executemany_bulk_insert_empty_params_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails with empty parameters."""
    with raises(
        ProgrammingError, match="bulk_insert requires at least one parameter set"
    ):
        cursor.executemany(
            "INSERT INTO test_table VALUES ($1)",
            [],
            bulk_insert=True,
        )
