import json
import re
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List
from unittest.mock import patch

from httpx import URL, HTTPStatusError, Request, StreamError, codes
from pytest import LogCaptureFixture, mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db import Connection, Cursor
from firebolt.common._types import ColType
from firebolt.common.constants import CursorState
from firebolt.common.row_set.types import Column
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


async def test_cursor_state(
    httpx_mock: HTTPXMock,
    mock_query: Callable,
    query_url: str,
    cursor: Cursor,
):
    """Cursor state changes depend on the operations performed with it."""
    mock_query()

    assert cursor._state == CursorState.NONE

    await cursor.execute("select")
    assert cursor._state == CursorState.DONE

    def error_query_callback(*args, **kwargs):
        raise Exception()

    httpx_mock.add_callback(error_query_callback, url=query_url, is_reusable=True)

    cursor._reset()
    with raises(Exception):
        await cursor.execute("select")
    assert cursor._state == CursorState.ERROR

    cursor._reset()
    assert cursor._state == CursorState.NONE

    cursor.close()
    assert cursor._state == CursorState.CLOSED


async def test_closed_cursor(cursor: Cursor):
    """Most cursor methods are unavailable for closed cursor."""
    fields = ("description", "rowcount", "statistics")
    async_methods = (
        ("execute", (cursor,)),
        ("executemany", (cursor, [])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
        ("nextset", ()),
    )
    methods = ("setinputsizes", "setoutputsize")

    cursor.close()

    for field in fields:
        with raises(CursorClosedError):
            getattr(cursor, field)

    for method in methods:
        with raises(CursorClosedError):
            getattr(cursor, method)(cursor)

    for amethod, args in async_methods:
        with raises(CursorClosedError):
            await getattr(cursor, amethod)(*args)

    with raises(CursorClosedError):
        async with cursor:
            pass

    with raises(CursorClosedError):
        [r async for r in cursor]

    # No errors
    assert cursor.closed
    cursor.close()


async def test_cursor_no_query(
    httpx_mock: HTTPXMock,
    mock_query: Callable,
    cursor: Cursor,
):
    """Some cursor methods are unavailable until a query is run."""
    async_methods = (
        "fetchone",
        "fetchmany",
        "fetchall",
    )

    mock_query()

    for amethod in async_methods:
        with raises(QueryNotRunError):
            await getattr(cursor, amethod)()

    with raises(QueryNotRunError):
        await cursor.nextset()

    with raises(QueryNotRunError):
        [r async for r in cursor]

    # No errors
    assert not cursor.description
    cursor._reset()
    assert cursor.rowcount == -1
    cursor._reset()
    assert not cursor.closed
    cursor._reset()
    await cursor.execute("select")
    cursor._reset()
    await cursor.executemany("select", [])
    cursor._reset()
    cursor.setinputsizes([0])
    cursor._reset()
    cursor.setoutputsize(0)
    # Context manager is also available
    async with cursor:
        pass
    # should this be available?
    # async with cursor:
    #     pass


async def test_cursor_execute(
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
        assert await query() == len(
            python_query_data
        ), f"Invalid row count returned for {message}."
        assert cursor.rowcount == len(
            python_query_data
        ), f"Invalid rowcount value for {message}."
        for i, (desc, exp) in enumerate(
            zip(cursor.description, python_query_description)
        ):
            assert desc == exp, f"Invalid column description at position {i}"

        for i in range(cursor.rowcount):
            assert (
                await cursor.fetchone() == python_query_data[i]
            ), f"Invalid data row at position {i} for {message}."

        assert (
            await cursor.fetchone() is None
        ), f"Non-empty fetchone after all data received {message}."

        # Query with empty output
        mock_insert_query()
        assert await query() == -1, f"Invalid row count for insert using {message}."
        assert (
            cursor.rowcount == -1
        ), f"Invalid rowcount value for insert using {message}."
        assert (
            cursor.description == []
        ), f"Invalid description for insert using {message}."


async def test_cursor_execute_error(
    httpx_mock: HTTPXMock,
    api_endpoint: str,
    query_url: str,
    db_name: str,
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
            await query()

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
            await query()

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
            await query()

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
            await query()
        assert cursor._state == CursorState.ERROR
        assert error_message in str(excinfo)

        httpx_mock.reset()


async def test_cursor_fetchone(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """cursor fetchone fetches single row in correct order. If no rows, returns None."""
    mock_query()

    await cursor.execute("sql")

    assert (await cursor.fetchone())[0] == 0, "Invalid rows order returned by fetchone."
    assert (await cursor.fetchone())[0] == 1, "Invalid rows order returned by fetchone."

    assert (
        len(await cursor.fetchall()) == cursor.rowcount - 2
    ), "Invalid row number returned by fetchall."

    assert (
        await cursor.fetchone() is None
    ), "fetchone should return None when no rows left to fetch."

    mock_insert_query()
    await cursor.execute("sql")
    with raises(DataError):
        await cursor.fetchone()


async def test_cursor_fetchmany(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """
    Cursor's fetchmany fetches the provided amount of rows, or arraysize by
    default. If not enough rows left, returns less or None if there are no rows.
    """
    mock_query()

    await cursor.execute("sql")

    with raises(TypeError) as excinfo:
        cursor.arraysize = "123"

    assert (
        len(await cursor.fetchmany(0)) == 0
    ), "Invalid count of rows returned by fetchmany for 0 size"

    assert (
        str(excinfo.value) == "Invalid arraysize value type, expected int, got str"
    ), "Invalid value error message"
    cursor.arraysize = 2

    many = await cursor.fetchmany()
    assert len(many) == cursor.arraysize, "Invalid count of rows returned by fetchmany"
    assert [r[0] for r in many] == [0, 1], "Invalid rows order returned by fetchmany"

    many = await cursor.fetchmany(cursor.arraysize + 3)
    assert (
        len(many) == cursor.arraysize + 3
    ), "Invalid count of rows returned by fetchmany with size provided"
    assert [r[0] for r in many] == list(
        range(2, 7)
    ), "Invalid rows order returned by fetchmany"

    # only 3 left at this point
    many = await cursor.fetchmany(4)
    assert (
        len(many) == 3
    ), "Invalid count of rows returned by fetchmany for last elements"
    assert [r[0] for r in many] == list(
        range(7, 10)
    ), "Invalid rows order returned by fetchmany"

    assert (
        len(await cursor.fetchmany()) == 0
    ), "fetchmany should return empty result set when no rows remain to fetch"

    mock_insert_query()
    await cursor.execute("sql")
    with raises(DataError):
        await cursor.fetchmany()


async def test_cursor_fetchall(
    mock_query: Callable,
    mock_insert_query: Callable,
    cursor: Cursor,
):
    """cursor fetchall fetches all rows remaining after last query."""
    mock_query()

    await cursor.execute("sql")

    await cursor.fetchmany(4)
    tail = await cursor.fetchall()
    assert (
        len(tail) == cursor.rowcount - 4
    ), "Invalid count of rows returned by fetchall"
    assert [r[0] for r in tail] == list(
        range(4, cursor.rowcount)
    ), "Invalid rows order returned by fetchall"

    assert (
        len(await cursor.fetchall()) == 0
    ), "fetchmany should return empty result set when no rows remain to fetch"

    mock_insert_query()
    await cursor.execute("sql")
    with raises(DataError):
        await cursor.fetchall()


async def test_cursor_multi_statement(
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

    rc = await cursor.execute(
        "select * from t; insert into t values (1, 2); select * from t"
    )
    assert rc == len(python_query_data), "Invalid row count returned"
    assert cursor.rowcount == len(python_query_data), "Invalid cursor row count"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    for i in range(cursor.rowcount):
        assert (
            await cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i}"

    assert await cursor.nextset()
    assert cursor.rowcount == -1, "Invalid cursor row count"
    assert cursor.description == [], "Invalid cursor description"
    assert cursor.statistics is None, "Invalid cursor statistics"

    with raises(DataError) as exc_info:
        await cursor.fetchall()

    assert str(exc_info.value) == "no rows to fetch", "Invalid error message"

    assert await cursor.nextset()

    assert cursor.rowcount == len(python_query_data), "Invalid cursor row count"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    assert cursor.statistics.elapsed == 0.116907717
    assert cursor.statistics.time_before_execution == 0.012180623
    assert cursor.statistics.time_to_execute == 0.104614307
    assert cursor.statistics.rows_read == 1
    assert cursor.statistics.bytes_read == 61
    assert cursor.statistics.scanned_bytes_cache == 0
    assert cursor.statistics.scanned_bytes_storage == 0

    for i in range(cursor.rowcount):
        assert (
            await cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i}"

    assert await cursor.nextset() is False


async def test_cursor_set_statements(
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

    rc = await cursor.execute("set a = b")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        await cursor.fetchall()

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

    rc = await cursor.execute("set param1=1")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        await cursor.fetchall()

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

    rc = await cursor.execute("set param2=0")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description == [], "Non-empty description for set"
    with raises(DataError):
        await cursor.fetchall()

    assert len(cursor._set_parameters) == 2

    assert (
        "param1" in cursor._set_parameters and cursor._set_parameters["param1"] == "1"
    )

    assert (
        "param2" in cursor._set_parameters and cursor._set_parameters["param2"] == "0"
    )

    cursor.flush_parameters()

    assert len(cursor._set_parameters) == 0


async def test_cursor_set_parameters_sent(
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
        await cursor.execute(f"set {p} = {v}")

    httpx_mock.add_callback(
        query_with_params_callback,
        url=f"{query_url}{params}",
        is_reusable=True,
    )
    await cursor.execute("select 1")


async def test_cursor_skip_parse(
    mock_query: Callable,
    cursor: Cursor,
):
    """Cursor doesn't process a query if skip_parsing is provided."""
    mock_query()

    with patch(
        "firebolt.common.statement_formatter.StatementFormatter.split_format_sql",
        return_value=["sql"],
    ) as split_format_sql_mock:
        await cursor.execute("non-an-actual-sql")
        split_format_sql_mock.assert_called_once()

    with patch(
        "firebolt.common.statement_formatter.StatementFormatter.split_format_sql"
    ) as split_format_sql_mock:
        await cursor.execute("non-an-actual-sql", skip_parsing=True)
        split_format_sql_mock.assert_not_called()


async def test_cursor_iterate(
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    httpx_mock.add_callback(query_callback, url=query_url, is_reusable=True)

    with raises(QueryNotRunError):
        async for res in cursor:
            pass

    await cursor.execute("select * from t")
    i = 0
    async for res in cursor:
        assert res in python_query_data
        i += 1
    assert i == len(python_query_data), "Wrong number iterations of a cursor were done"

    cursor.close()
    with raises(CursorClosedError):
        async for res in cursor:
            pass


async def test_server_side_header_database(
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
    await cursor.execute(f"USE DATABASE = '{db_name_updated}'")
    assert cursor.database == db_name_updated

    httpx_mock.reset()
    # Check updated database is used in the next query
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=query_url_updated,
        is_reusable=True,
    )
    await cursor.execute("select 1")
    assert cursor.database == db_name_updated


async def test_cursor_unknown_error_body_logging(
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
        await cursor.execute("select 1")
    assert actual_error_body in caplog.text


@mark.parametrize(
    "parameter",
    [
        "database",
        "engine",
        "output_format",
    ],
)
async def test_disallowed_set_parameter(cursor: Cursor, parameter: str) -> None:
    """Test that setting disallowed parameters raises an error."""
    with raises(ConfigurationError) as e:
        await cursor.execute(f"SET {parameter}=dummy")
    assert f"Set parameter '{parameter}' is not allowed" in str(
        e.value
    ), "invalid error"
    assert cursor._set_parameters == {}, "set parameters should not be updated"


async def test_cursor_use_engine_no_parameters(
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
    await cursor.execute("USE ENGINE = 'my_dummy_engine'")
    assert cursor.engine_url == f"https://{query_updated_url}"

    httpx_mock.reset()
    # Check updated engine is used in the next query
    new_url = query_url.copy_with(host=query_updated_url)
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=new_url,
        is_reusable=True,
    )
    await cursor.execute("select 1")
    assert cursor.engine_url == f"https://{query_updated_url}"


async def test_cursor_use_engine_with_parameters(
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
    await cursor.execute("USE ENGINE = 'my_dummy_engine'")
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
    await cursor.execute("select 1")
    assert cursor.engine_url == f"https://{query_updated_url}"


async def test_cursor_reset_session(
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

    await cursor.execute("set a = b")
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
    await cursor.execute("SELECT 1")
    assert len(cursor._set_parameters) == 0
    assert bool(cursor.engine_url) is True, "engine url is not set"
    assert bool(cursor.database) is True, "database is not set"


async def test_cursor_remove_parameters_header(
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
    await cursor.execute("set param1 = value1")
    await cursor.execute("set param2 = value2")
    await cursor.execute("set param3 = value3")

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
    await cursor.execute("SELECT 1")

    # Verify that param1 and param3 were removed, param2 remains
    assert len(cursor._set_parameters) == 1
    assert "param1" not in cursor._set_parameters
    assert "param2" in cursor._set_parameters
    assert "param3" not in cursor._set_parameters
    assert cursor._set_parameters["param2"] == "value2"


async def test_cursor_timeout(
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
        await cursor.execute("SELECT 1; SELECT 2", timeout_seconds=1)

    assert long_executed is True, "long query was executed"
    assert fast_executed is False, "fast query was not executed"

    httpx_mock.reset()


async def verify_async_fetch_not_allowed(cursor: Cursor):
    with raises(MethodNotAllowedInAsyncError):
        await cursor.fetchall()
    with raises(MethodNotAllowedInAsyncError):
        await cursor.fetchone()
    with raises(MethodNotAllowedInAsyncError):
        await cursor.fetchmany()


async def test_cursor_execute_async(
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
    await cursor.execute_async("SELECT 2")
    await verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


async def test_cursor_execute_async_multiple_queries(
    cursor: Cursor,
):
    with raises(FireboltError) as e:
        await cursor.execute_async("SELECT 2; SELECT 3")
    assert "does not support multi-statement" in str(e.value)


async def test_cursor_execute_async_parametrised_query(
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
    await cursor.execute_async("SELECT 2 WHERE x = ?", [1])
    await verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


async def test_cursor_execute_async_skip_parsing(
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
    await cursor.execute_async("SELECT 2; SELECT 3", skip_parsing=True)
    await verify_async_fetch_not_allowed(cursor)
    assert cursor.async_query_token == async_token
    assert cursor._state == CursorState.DONE


async def test_cursor_execute_async_validate_set_parameters(
    cursor: Cursor,
):
    with raises(FireboltError) as e:
        await cursor.execute_async("SET a = b")
    assert "does not support set" in str(e.value)


async def test_cursor_execute_async_respects_api_errors(
    httpx_mock: HTTPXMock,
    async_query_url: str,
    cursor: Cursor,
):
    httpx_mock.add_callback(
        lambda *args, **kwargs: Response(status_code=codes.BAD_REQUEST),
        url=async_query_url,
    )
    with raises(HTTPStatusError):
        await cursor.execute_async("SELECT 2")


async def test_cursor_execute_stream(
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
    await cursor.execute_stream("select * from large_table")
    assert (
        cursor.rowcount == -1
    ), f"Expected row count to be -1 until the end of streaming for execution with streaming"
    for i, (desc, exp) in enumerate(zip(cursor.description, python_query_description)):
        assert desc == exp, f"Invalid column description at position {i}"

    for i in range(len(python_query_data)):
        assert (
            await cursor.fetchone() == python_query_data[i]
        ), f"Invalid data row at position {i} for execution with streaming."

    assert (
        await cursor.fetchone() is None
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
    await cursor.execute_stream("insert into t values (1, 2)")
    assert (
        cursor.rowcount == -1
    ), f"Invalid rowcount value for insert using execution with streaming."
    assert (
        cursor.description == []
    ), f"Invalid description for insert using execution with streaming."
    assert (
        await cursor.fetchone() is None
    ), f"Invalid statistics for insert using execution with streaming."


async def test_cursor_execute_stream_error(
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
        await cursor.execute_stream("select * from large_table")

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
        await cursor.execute_stream("select * from large_table")

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
        await cursor.execute_stream("select * from large_table")

        # Error is raised during streaming
        with raises(FireboltStructuredError):
            await method()


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
async def test_fb_numeric_parameter_formatting(
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

    await cursor.execute(test_query, test_params)


async def test_fb_numeric_complex_types_converted_to_strings_async(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
):
    """Test that fb_numeric paramstyle converts complex types to strings in async mode (same as sync)."""
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

    import firebolt.async_db as async_db

    original_paramstyle = async_db.paramstyle
    try:
        async_db.paramstyle = "fb_numeric"
        await cursor.execute(test_query, test_params)
    finally:
        async_db.paramstyle = original_paramstyle


async def test_fb_numeric_no_client_side_substitution(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: URL,
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

    import firebolt.async_db as async_db

    original_paramstyle = async_db.paramstyle
    try:
        async_db.paramstyle = "fb_numeric"
        await cursor.execute(test_query, test_params)
    finally:
        async_db.paramstyle = original_paramstyle


async def test_fb_numeric_executemany(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: URL,
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

    async def validate_executemany_callback(request: Request, **kwargs):
        assert request.method == "POST"

        # Should process multiple parameter sets sequentially
        import json as json_mod
        from urllib.parse import parse_qs

        qs = parse_qs(request.url.query)
        query_params_raw = qs.get(b"query_parameters", [])

        if query_params_raw:
            query_params_str = query_params_raw[0].decode()
            actual_query_params = json_mod.loads(query_params_str)
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

    cursor.paramstyle = "fb_numeric"
    await cursor.executemany(test_query, test_params_seq)


async def test_fb_numeric_with_set_parameters(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: URL,
    fb_numeric_simple_callback: Callable,
):
    """Test that fb_numeric paramstyle works correctly when cursor has pre-existing set parameters."""
    cursor.paramstyle = "fb_numeric"

    # Manually set a parameter in the cursor (simulating what would happen
    # if SET was called in a different paramstyle mode)
    cursor._set_parameters = {"my_param": "test_value"}

    test_query = "SELECT * FROM test WHERE id = $1"
    test_params = [42]

    async def validate_with_set_params_callback(request: Request, **kwargs):
        assert request.method == "POST"

        # Should include both set parameters and query parameters
        from urllib.parse import parse_qs

        qs = parse_qs(request.url.query)

        # Check for set parameter
        assert b"my_param" in qs
        assert qs[b"my_param"] == [b"test_value"]

        # Check for query parameters
        query_params_raw = qs.get(b"query_parameters", [])
        if query_params_raw:
            import json as json_mod

            query_params_str = query_params_raw[0].decode()
            actual_query_params = json_mod.loads(query_params_str)
            expected = [{"name": "$1", "value": 42}]
            assert actual_query_params == expected

        return fb_numeric_simple_callback(request, **kwargs)

    # Mock the SELECT query with set parameters
    httpx_mock.add_callback(
        validate_with_set_params_callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    await cursor.execute(test_query, test_params)


async def test_fb_numeric_execute_async(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: URL,
    fb_numeric_async_callback: Callable,
    async_token: str,
):
    """Test that fb_numeric paramstyle works with execute_async method."""
    test_query = "SELECT * FROM test WHERE id = $1 AND name = $2"
    test_params = [42, "async_test"]

    async def validate_async_callback(request: Request, **kwargs) -> Response:
        assert request.method == "POST"

        # Should include async=True parameter
        from urllib.parse import parse_qs

        qs = parse_qs(request.url.query)
        async_param = qs.get(b"async", [])
        assert async_param == [b"true"], f"Expected async=true, got: {async_param}"

        # Should include query parameters
        query_params_raw = qs.get(b"query_parameters", [])
        if query_params_raw:
            import json as json_mod

            query_params_str = query_params_raw[0].decode()
            actual_query_params = json_mod.loads(query_params_str)
            expected = [
                {"name": "$1", "value": 42},
                {"name": "$2", "value": "async_test"},
            ]
            assert actual_query_params == expected

        return fb_numeric_async_callback(request, **kwargs)

    httpx_mock.add_callback(
        validate_async_callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    cursor.paramstyle = "fb_numeric"
    result = await cursor.execute_async(test_query, test_params)

    assert result == -1  # execute_async always returns -1
    assert cursor.async_query_token == async_token


async def test_fb_numeric_execute_stream(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: URL,
    streaming_query_callback: Callable,
):
    """Test that fb_numeric paramstyle works with execute_stream method."""
    test_query = "SELECT * FROM large_table WHERE id = $1"
    test_params = [42]

    async def validate_streaming_callback(request: Request, **kwargs) -> Response:
        assert request.method == "POST"

        # Should include streaming output format
        from urllib.parse import parse_qs

        qs = parse_qs(request.url.query)
        output_format = qs.get(b"output_format", [])
        assert output_format == [
            b"JSONLines_Compact"
        ], f"Expected JSONLines_Compact output format, got: {output_format}"

        # Should include query parameters
        query_params_raw = qs.get(b"query_parameters", [])
        if query_params_raw:
            import json as json_mod

            query_params_str = query_params_raw[0].decode()
            actual_query_params = json_mod.loads(query_params_str)
            expected = [{"name": "$1", "value": 42}]
            assert actual_query_params == expected

        return streaming_query_callback(request, **kwargs)

    # Mock all fb_numeric URL patterns (the regex pattern covers all variations)
    httpx_mock.add_callback(
        validate_streaming_callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    cursor.paramstyle = "fb_numeric"
    await cursor.execute_stream(test_query, test_params)


@mark.parametrize(
    "test_params,expected_query_params",
    [
        # Decimal types - now converted to strings consistently
        (
            [Decimal("123.45"), Decimal("0"), Decimal("-999.999")],
            [
                {"name": "$1", "value": "123.45"},
                {"name": "$2", "value": "0"},
                {"name": "$3", "value": "-999.999"},
            ],
        ),
        # Bytes values - now converted to strings consistently
        (
            [b"hello", b"\x00\x01\x02", b""],
            [
                {"name": "$1", "value": "hello"},
                {"name": "$2", "value": "\x00\x01\x02"},
                {"name": "$3", "value": ""},
            ],
        ),
        # List/Array values - now converted to strings consistently
        (
            [[1, 2, 3], ["a", "b"], [], [None, True, False]],
            [
                {"name": "$1", "value": [1, 2, 3]},
                {"name": "$2", "value": ["a", "b"]},
                {"name": "$3", "value": []},
                {"name": "$4", "value": [None, True, False]},
            ],
        ),
        # Mixed complex types - now converted to strings consistently
        (
            [Decimal("42.0"), b"binary", [1, "mixed"], {"key": "value"}],
            [
                {"name": "$1", "value": "42.0"},
                {"name": "$2", "value": "binary"},
                {"name": "$3", "value": [1, "mixed"]},
                {"name": "$4", "value": "{'key': 'value'}"},
            ],
        ),
    ],
)
async def test_fb_numeric_additional_types_unified_behavior(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    test_params: List[Any],
    expected_query_params: List[Dict[str, Any]],
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle handles additional types consistently in async mode (same as sync)."""
    test_query = f"SELECT * FROM test WHERE col IN ({', '.join(f'${i+1}' for i in range(len(test_params)))})"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    await cursor.execute(test_query, test_params)


async def test_fb_numeric_mixed_basic_and_complex_types(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle works with mixed basic and complex types in async mode."""
    # Mix of basic types (preserved) and complex types (converted to strings)
    test_params = [[1, 2, 3], 42, {"key": "value", "number": 42}, None]
    expected_query_params = [
        {"name": "$1", "value": [1, 2, 3]},
        {"name": "$2", "value": 42},
        {"name": "$3", "value": "{'key': 'value', 'number': 42}"},
        {"name": "$4", "value": None},
    ]

    test_query = (
        "SELECT * FROM test WHERE data = $1 AND id = $2 AND meta = $3 AND null_val = $4"
    )

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    await cursor.execute(test_query, test_params)


async def test_fb_numeric_large_parameter_count_async(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle handles a large number of parameters in async mode."""
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

    await cursor.execute(test_query, test_params)


async def test_fb_numeric_special_float_values_async(
    cursor: Cursor,
    httpx_mock: HTTPXMock,
    fb_numeric_query_url: re.Pattern,
    fb_numeric_callback_factory: Callable,
    fb_numeric_paramstyle,
):
    """Test that fb_numeric paramstyle handles special float values in async mode."""
    test_params = [
        float("inf"),
        float("-inf"),
        2**63 - 1,  # Large integer
        -(2**63),  # Very negative integer
    ]
    expected_query_params = [
        {"name": "$1", "value": float("inf")},  # JSON can handle Infinity
        {"name": "$2", "value": float("-inf")},  # JSON can handle -Infinity
        {"name": "$3", "value": 9223372036854775807},
        {"name": "$4", "value": -9223372036854775808},
    ]

    test_query = f"SELECT * FROM test WHERE col IN ({', '.join(f'${i+1}' for i in range(len(test_params)))})"

    callback = fb_numeric_callback_factory(expected_query_params, test_query)
    httpx_mock.add_callback(
        callback,
        url=fb_numeric_query_url,
        is_reusable=True,
    )

    await cursor.execute(test_query, test_params)


async def test_unsupported_paramstyle_raises(cursor: Cursor) -> None:
    """Test that unsupported paramstyles raise ProgrammingError."""
    import firebolt.async_db as db

    original_paramstyle = db.paramstyle
    try:
        db.paramstyle = "not_a_style"
        with raises(ProgrammingError):
            await cursor.execute("SELECT 1")
    finally:
        db.paramstyle = original_paramstyle


async def test_executemany_bulk_insert_qmark_works(
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

    result = await cursor.executemany(
        "INSERT INTO test_table VALUES (?, ?)",
        [(1, "a"), (2, "b"), (3, "c")],
        bulk_insert=True,
    )
    assert result == 0


async def test_executemany_bulk_insert_fb_numeric(
    httpx_mock: HTTPXMock,
    cursor: Cursor,
    query_url: str,
):
    """executemany with bulk_insert=True and FB_NUMERIC style."""
    import firebolt.async_db as db_module

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

        result = await cursor.executemany(
            "INSERT INTO test_table VALUES ($1, $2)",
            [(1, "a"), (2, "b"), (3, "c")],
            bulk_insert=True,
        )
        assert result == 0
    finally:
        db_module.paramstyle = original_paramstyle


async def test_executemany_bulk_insert_non_insert_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails for non-INSERT queries."""
    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        await cursor.executemany(
            "SELECT * FROM test_table",
            [()],
            bulk_insert=True,
        )

    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        await cursor.executemany(
            "UPDATE test_table SET col = $1",
            [(1,)],
            bulk_insert=True,
        )

    with raises(ConfigurationError, match="bulk_insert is only supported for INSERT"):
        await cursor.executemany(
            "DELETE FROM test_table WHERE id = $1",
            [(1,)],
            bulk_insert=True,
        )


async def test_executemany_bulk_insert_multi_statement_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails for multi-statement queries."""
    with raises(
        ProgrammingError, match="bulk_insert does not support multi-statement queries"
    ):
        await cursor.executemany(
            "INSERT INTO test_table VALUES ($1); SELECT * FROM test_table",
            [(1,)],
            bulk_insert=True,
        )

    with raises(
        ProgrammingError, match="bulk_insert does not support multi-statement queries"
    ):
        await cursor.executemany(
            "INSERT INTO test_table VALUES ($1); INSERT INTO test_table VALUES ($2)",
            [(1,), (2,)],
            bulk_insert=True,
        )


async def test_executemany_bulk_insert_empty_params_fails(
    cursor: Cursor, fb_numeric_paramstyle
):
    """executemany with bulk_insert=True fails with empty parameters."""
    with raises(ProgrammingError, match="requires at least one parameter set"):
        await cursor.executemany(
            "INSERT INTO test_table VALUES ($1)",
            [],
            bulk_insert=True,
        )


# Transaction tests


async def test_autocommit_off_triggers_implicit_transaction_start(
    httpx_mock: HTTPXMock,
    connection_autocommit_off: Connection,
    begin_transaction_callback: Callable,
    select_one_query_callback: Callable,
    commit_transaction_callback: Callable,
    transaction_id: str,
):
    """Test that transaction is implicitly started when autocommit=False."""

    httpx_mock.add_callback(begin_transaction_callback, method="POST")
    httpx_mock.add_callback(select_one_query_callback, method="POST")
    httpx_mock.add_callback(commit_transaction_callback, method="POST")

    cursor = connection_autocommit_off.cursor()
    # Connection should not be in transaction initially
    assert cursor.connection.in_transaction is False
    assert cursor.connection._transaction_id is None

    # Execute a regular query - this should implicitly start the transaction
    result = await cursor.execute("SELECT 1")

    # Connection should now be in transaction
    assert cursor.connection.in_transaction is True
    assert cursor.connection._transaction_id == transaction_id
    assert result == 1  # SELECT 1 returns 1 row


async def test_connection_commit_clears_transaction_state(
    httpx_mock: HTTPXMock,
    connection_autocommit_off: Connection,
    begin_transaction_callback: Callable,
    transaction_query_callback: Callable,
    commit_transaction_callback: Callable,
    transaction_id: str,
):
    """Test that COMMIT transaction is executed and connection state is cleared."""
    # Start transaction implicitly with a query
    httpx_mock.add_callback(
        begin_transaction_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )

    cursor = connection_autocommit_off.cursor()
    await cursor.execute("SELECT 1")  # This should implicitly start transaction
    assert cursor.connection.in_transaction is True

    # Now commit using connection method
    httpx_mock.reset()
    httpx_mock.add_callback(
        commit_transaction_callback,
        method="POST",
    )

    await connection_autocommit_off.commit()

    # Connection should no longer be in transaction
    assert cursor.connection.in_transaction is False
    assert cursor.connection._transaction_id is None
    assert cursor.connection._transaction_sequence_id is None


async def test_connection_rollback_clears_transaction_state(
    httpx_mock: HTTPXMock,
    connection_autocommit_off: Connection,
    begin_transaction_callback: Callable,
    transaction_query_callback: Callable,
    rollback_transaction_callback: Callable,
    transaction_id: str,
):
    """Test that ROLLBACK transaction is executed and connection state is cleared."""
    # Start transaction implicitly with a query
    httpx_mock.add_callback(
        begin_transaction_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )

    cursor = connection_autocommit_off.cursor()
    await cursor.execute("SELECT 1")  # This should implicitly start transaction
    assert cursor.connection.in_transaction is True

    # Now rollback using connection method
    httpx_mock.reset()
    httpx_mock.add_callback(
        rollback_transaction_callback,
        method="POST",
    )

    await connection_autocommit_off.rollback()

    # Connection should no longer be in transaction
    assert cursor.connection.in_transaction is False
    assert cursor.connection._transaction_id is None
    assert cursor.connection._transaction_sequence_id is None


async def test_transaction_sequence_id_changes_each_query(
    httpx_mock: HTTPXMock,
    connection_autocommit_off: Connection,
    begin_transaction_callback: Callable,
    transaction_query_callback: Callable,
    commit_transaction_callback: Callable,
    transaction_id: str,
    transaction_sequence_id: int,
):
    """Test that transaction sequence id increments with each query in transaction."""
    # Start transaction implicitly
    httpx_mock.add_callback(
        begin_transaction_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        commit_transaction_callback,
        method="POST",
    )

    cursor = connection_autocommit_off.cursor()
    await cursor.execute("SELECT 1")  # This should implicitly start transaction
    assert cursor.connection._transaction_id == transaction_id

    # Execute second query in transaction
    httpx_mock.reset()
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        commit_transaction_callback,
        method="POST",
    )

    await cursor.execute("SELECT 2")

    # Sequence id should be incremented
    assert cursor.connection._transaction_sequence_id == str(
        transaction_sequence_id + 1
    )


async def test_transaction_params_included_in_query_requests(
    httpx_mock: HTTPXMock,
    connection_autocommit_off: Connection,
    begin_transaction_callback: Callable,
    transaction_query_callback: Callable,
    commit_transaction_callback: Callable,
):
    """Test that transaction parameters are correctly passed in query URLs."""
    # Start transaction implicitly
    httpx_mock.add_callback(
        begin_transaction_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        commit_transaction_callback,
        method="POST",
    )

    cursor = connection_autocommit_off.cursor()
    # Execute query - this should implicitly start transaction and include transaction params
    await cursor.execute("SELECT 1")

    # Execute second query in transaction - callback will verify transaction params are present
    httpx_mock.reset()
    httpx_mock.add_callback(
        transaction_query_callback,
        method="POST",
    )
    httpx_mock.add_callback(
        commit_transaction_callback,
        method="POST",
    )

    await cursor.execute("SELECT 2")
