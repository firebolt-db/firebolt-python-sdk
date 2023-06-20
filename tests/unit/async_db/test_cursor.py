from typing import Any, Callable, Dict, List
from unittest.mock import patch

from httpx import HTTPStatusError, StreamError, codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.async_db import Cursor
from firebolt.common._types import Column
from firebolt.common.base_cursor import ColType, CursorState, QueryStatus
from firebolt.common.settings import Settings
from firebolt.utils.exception import (
    AsyncExecutionUnavailableError,
    CursorClosedError,
    DataError,
    EngineNotRunningError,
    FireboltDatabaseError,
    FireboltEngineError,
    OperationalError,
    ProgrammingError,
    QueryNotRunError,
)
from tests.unit.db_conftest import encode_param


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

    httpx_mock.add_callback(error_query_callback, url=query_url)

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
        with cursor:
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
    with cursor:
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
            cursor.description is None
        ), f"Invalid description for insert using {message}."


async def test_cursor_execute_error(
    httpx_mock: HTTPXMock,
    query_url: str,
    get_engines_url: str,
    settings: Settings,
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

        httpx_mock.add_callback(http_error, url=query_url)
        with raises(StreamError) as excinfo:
            await query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "httpx error"
        ), f"Invalid query error message for {message}."

        # HTTP error
        httpx_mock.add_response(status_code=codes.BAD_REQUEST, url=query_url)
        with raises(HTTPStatusError) as excinfo:
            await query()

        errmsg = str(excinfo.value)
        assert cursor._state == CursorState.ERROR
        assert "Bad Request" in errmsg, f"Invalid query error message for {message}."

        # Database query error
        httpx_mock.add_response(
            status_code=codes.INTERNAL_SERVER_ERROR,
            content="Query error message",
            url=query_url,
        )
        with raises(OperationalError) as excinfo:
            await query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "Error executing query:\nQuery error message"
        ), f"Invalid authentication error message for {message}."

        # Database does not exist error
        httpx_mock.add_response(
            status_code=codes.FORBIDDEN,
            content="Query error message",
            url=query_url,
            match_content=b"select * from t",
        )
        httpx_mock.add_response(
            url=system_engine_query_url,
            method="POST",
            json={
                "rows": "0",
                "data": [],
                "meta": [],
                "statistics": query_statistics,
            },
        )
        with raises(FireboltDatabaseError) as excinfo:
            await query()
        assert cursor._state == CursorState.ERROR
        assert db_name in str(excinfo)

        # Database exists but some other error
        error_message = "My query error message"
        httpx_mock.add_response(
            status_code=codes.FORBIDDEN,
            content=error_message,
            url=query_url,
            match_content=b"select * from t",
        )
        httpx_mock.add_response(
            url=system_engine_query_url,
            method="POST",
            json={
                "rows": "1",
                "data": ["my_db"],
                "meta": [],
                "statistics": query_statistics,
            },
        )
        with raises(ProgrammingError) as excinfo:
            await query()
        assert cursor._state == CursorState.ERROR
        assert error_message in str(excinfo)

        # Engine is not running error
        httpx_mock.add_response(
            status_code=codes.SERVICE_UNAVAILABLE,
            content="Query error message",
            url=query_url,
        )
        httpx_mock.add_response(
            url=system_engine_query_url,
            method="POST",
            json={
                "rows": "1",
                "data": [[get_engines_url, "my_db", "Stopped"]],
                "meta": [
                    {"name": "url", "type": "text"},
                    {"name": "attached_to", "type": "text"},
                    {"name": "status", "type": "text"},
                ],
                "statistics": query_statistics,
            },
        )
        with raises(EngineNotRunningError) as excinfo:
            await query()
        assert cursor._state == CursorState.ERROR
        assert settings.server in str(excinfo)

        # Engine does not exist
        httpx_mock.add_response(
            status_code=codes.SERVICE_UNAVAILABLE,
            content="Query error message",
            url=query_url,
        )
        httpx_mock.add_response(
            url=system_engine_query_url,
            method="POST",
            json={
                "rows": "0",
                "data": [],
                "meta": [],
                "statistics": query_statistics,
            },
        )
        with raises(FireboltEngineError) as excinfo:
            await query()
        assert cursor._state == CursorState.ERROR

        httpx_mock.reset(True)


async def test_cursor_server_side_async_execute_errors(
    httpx_mock: HTTPXMock,
    query_with_params_url: str,
    server_side_async_missing_id_callback: Callable,
    insert_query_callback: str,
    cursor: Cursor,
):
    """
    Cursor handles all types of errors properly using server-side
    async queries.
    """
    for query, message in (
        (
            lambda sql: cursor.execute(sql, async_execution=True),
            "server-side asynchronous execute()",
        ),
        (
            lambda sql: cursor.executemany(sql, [], async_execution=True),
            "server-side asynchronous executemany()",
        ),
    ):
        httpx_mock.add_callback(insert_query_callback, url=query_with_params_url)
        with raises(OperationalError) as excinfo:
            await query("SELECT * FROM t")

        assert cursor._state == CursorState.ERROR
        assert str(excinfo.value) == ("No response to asynchronous query.")

        # Missing query_id from server-side asynchronous execution.
        httpx_mock.add_callback(
            server_side_async_missing_id_callback, url=query_with_params_url
        )
        with raises(OperationalError) as excinfo:
            await query("SELECT * FROM t")

        assert cursor._state == CursorState.ERROR
        assert str(excinfo.value) == (
            "Invalid response to asynchronous query: missing query_id."
        )

        # Multi-statement queries are not possible with async_execution error.
        with raises(AsyncExecutionUnavailableError) as excinfo:
            await query("SELECT * FROM t; SELECT * FROM s")

        assert cursor._state == CursorState.ERROR
        assert str(excinfo.value) == (
            "It is not possible to execute multi-statement queries asynchronously."
        ), f"Multi-statement query was allowed for {message}."

        # Error out if async_execution is set via SET statement.
        with raises(AsyncExecutionUnavailableError) as excinfo:
            await cursor.execute("SET async_execution=1")

        assert cursor._state == CursorState.ERROR
        assert str(excinfo.value) == (
            "It is not possible to set async_execution using a SET command. "
            "Instead, pass it as an argument to the execute() or "
            "executemany() function."
        ), f"async_execution was allowed via a SET parameter on {message}."

        # Error out when doing async_execution and use_standard_sql are off.
        with raises(AsyncExecutionUnavailableError) as excinfo:
            await cursor.execute(
                "SET use_standard_sql=0; SELECT * FROM t", async_execution=True
            )

        assert cursor._state == CursorState.ERROR
        assert str(excinfo.value) == (
            "It is not possible to execute queries asynchronously if "
            "use_standard_sql=0."
        ), f"use_standard_sql=0 was allowed for server-side asynchronous queries on {message}."

        # Have to reauth or next execute fails. Not sure why.
        await cursor.execute("set use_standard_sql=1")
        httpx_mock.reset(True)


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
    assert cursor.description is None, "Invalid cursor description"
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

    assert await cursor.nextset() is None


async def test_cursor_set_statements(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    set_query_url: str,
    cursor: Cursor,
):
    """cursor correctly parses and processes set statements."""
    httpx_mock.add_callback(select_one_query_callback, url=f"{set_query_url}&a=b")

    assert len(cursor._set_parameters) == 0

    rc = await cursor.execute("set a = b")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
    with raises(DataError):
        await cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "a" in cursor._set_parameters
        and cursor._set_parameters["a"] == "b"
    )

    cursor.flush_parameters()

    assert len(cursor._set_parameters) == 0

    httpx_mock.add_callback(select_one_query_callback, url=f"{set_query_url}&param1=1")

    rc = await cursor.execute("set param1=1")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
    with raises(DataError):
        await cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "param1" in cursor._set_parameters
        and cursor._set_parameters["param1"] == "1"
    )

    httpx_mock.add_callback(
        select_one_query_callback, url=f"{set_query_url}&param1=1&param2=0"
    )

    rc = await cursor.execute("set param2=0")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
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
            select_one_query_callback, url=f"{set_query_url}{params}"
        )
        await cursor.execute(f"set {p} = {v}")

    httpx_mock.add_callback(query_with_params_callback, url=f"{query_url}{params}")
    await cursor.execute("select 1")


async def test_cursor_skip_parse(
    mock_query: Callable,
    cursor: Cursor,
):
    """Cursor doesn't process a query if skip_parsing is provided."""
    mock_query()

    with patch("firebolt.async_db.cursor.split_format_sql") as split_format_sql_mock:
        await cursor.execute("non-an-actual-sql")
        split_format_sql_mock.assert_called_once()

    with patch("firebolt.async_db.cursor.split_format_sql") as split_format_sql_mock:
        await cursor.execute("non-an-actual-sql", skip_parsing=True)
        split_format_sql_mock.assert_not_called()


async def test_cursor_server_side_async_execute(
    httpx_mock: HTTPXMock,
    server_side_async_id_callback: Callable,
    server_side_async_id: Callable,
    query_with_params_url: str,
    cursor: Cursor,
):
    """
    Cursor is able to execute query server-side asynchronously and
    query_id is returned.
    """
    for query, message in (
        (
            lambda: cursor.execute("select * from t", async_execution=True),
            "server-side asynchronous execute()",
        ),
        (
            lambda: cursor.executemany(
                "select * from t", parameters_seq=[], async_execution=True
            ),
            "server-side asynchronous executemany()",
        ),
    ):
        # Query with json output
        httpx_mock.add_callback(
            server_side_async_id_callback, url=query_with_params_url
        )

        assert (
            await query() == server_side_async_id
        ), f"Invalid query id returned for {message}."
        assert (
            cursor.rowcount == -1
        ), f"Invalid rowcount value for insert using {message}."
        assert (
            cursor.description is None
        ), f"Invalid description for insert using {message}."


async def test_cursor_server_side_async_cancel(
    httpx_mock: HTTPXMock,
    server_side_async_cancel_callback: Callable,
    server_side_async_id: Callable,
    query_with_params_url: str,
    cursor: Cursor,
):
    """
    Cursor is able to cancel query server-side asynchronously and
    query_id is returned.
    """

    # Query with json output
    httpx_mock.add_callback(
        server_side_async_cancel_callback, url=query_with_params_url
    )
    cursor._set_parameters = {"invalid_parameter": "should_not_be_present"}
    await cursor.cancel(server_side_async_id)
    cursor.close()
    with raises(CursorClosedError):
        await cursor.cancel(server_side_async_id)


async def test_cursor_server_side_async_get_status_completed(
    httpx_mock: HTTPXMock,
    server_side_async_get_status_callback: Callable,
    server_side_async_id: Callable,
    query_with_params_url: str,
    cursor: Cursor,
):
    """
    Cursor is able to execute query server-side asynchronously and
    query_id is returned.
    """

    # Query with json output
    httpx_mock.add_callback(
        server_side_async_get_status_callback, url=query_with_params_url
    )
    status = await cursor.get_status(server_side_async_id)
    assert status == QueryStatus.ENDED_SUCCESSFULLY


async def test_cursor_server_side_async_get_status_not_yet_available(
    httpx_mock: HTTPXMock,
    server_side_async_get_status_not_yet_availabe_callback: Callable,
    server_side_async_id: Callable,
    query_with_params_url: str,
    cursor: Cursor,
):
    """
    Cursor is able to execute query server-side asynchronously and
    query_id is returned.
    """

    # Query with json output
    httpx_mock.add_callback(
        server_side_async_get_status_not_yet_availabe_callback,
        url=query_with_params_url,
    )
    status = await cursor.get_status(server_side_async_id)
    assert status == QueryStatus.NOT_READY


async def test_cursor_server_side_async_get_status_error(
    httpx_mock: HTTPXMock,
    server_side_async_get_status_error: Callable,
    server_side_async_id: Callable,
    query_with_params_url: str,
    cursor: Cursor,
):
    """ """
    httpx_mock.add_callback(
        server_side_async_get_status_error, url=query_with_params_url
    )
    with raises(OperationalError) as excinfo:
        await cursor.get_status(server_side_async_id)

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value)
            == f"Asynchronous query {server_side_async_id} status check failed."
        ), f"Invalid get_status error message."


async def test_cursor_iterate(
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    httpx_mock.add_callback(query_callback, url=query_url)

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
