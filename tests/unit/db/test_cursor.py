from typing import Any, Callable, Dict, List
from unittest.mock import patch

from httpx import HTTPStatusError, StreamError, codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.common.settings import Settings
from firebolt.db import Cursor
from firebolt.db.cursor import ColType, Column, CursorState, QueryStatus
from firebolt.utils.exception import (
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

    httpx_mock.add_callback(error_query_callback, url=query_url)

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
            cursor.description is None
        ), f"Invalid description for insert using {message}."


def test_cursor_execute_error(
    httpx_mock: HTTPXMock,
    get_engines_url: str,
    settings: Settings,
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

        httpx_mock.add_callback(http_error, url=query_url)
        with raises(StreamError) as excinfo:
            query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "httpx error"
        ), f"Invalid query error message for {message}."

        # HTTP error
        httpx_mock.add_response(status_code=codes.BAD_REQUEST, url=query_url)
        with raises(HTTPStatusError) as excinfo:
            query()

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
            query()

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value) == "Error executing query:\nQuery error message"
        ), f"Invalid authentication error message for {message}."

        # Database does not exist error
        httpx_mock.add_response(
            status_code=codes.FORBIDDEN,
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
        with raises(FireboltDatabaseError) as excinfo:
            query()
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
            query()
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
            query()
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
            query()
        assert cursor._state == CursorState.ERROR

        httpx_mock.reset(True)


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
    assert cursor.description is None, "Invalid cursor description"
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

    assert cursor.nextset() is None


def test_cursor_set_statements(
    httpx_mock: HTTPXMock,
    select_one_query_callback: Callable,
    set_query_url: str,
    cursor: Cursor,
):
    """cursor correctly parses and processes set statements."""
    httpx_mock.add_callback(select_one_query_callback, url=f"{set_query_url}&a=b")

    assert len(cursor._set_parameters) == 0

    rc = cursor.execute("set a = b")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
    with raises(DataError):
        cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "a" in cursor._set_parameters
        and cursor._set_parameters["a"] == "b"
    )

    cursor.flush_parameters()

    assert len(cursor._set_parameters) == 0

    httpx_mock.add_callback(select_one_query_callback, url=f"{set_query_url}&param1=1")

    rc = cursor.execute("set param1=1")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
    with raises(DataError):
        cursor.fetchall()

    assert (
        len(cursor._set_parameters) == 1
        and "param1" in cursor._set_parameters
        and cursor._set_parameters["param1"] == "1"
    )

    httpx_mock.add_callback(
        select_one_query_callback, url=f"{set_query_url}&param1=1&param2=0"
    )

    rc = cursor.execute("set param2=0")
    assert rc == -1, "Invalid row count returned"
    assert cursor.description is None, "Non-empty description for set"
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
            select_one_query_callback, url=f"{set_query_url}{params}"
        )
        cursor.execute(f"set {p} = {v}")

    httpx_mock.add_callback(query_with_params_callback, url=f"{query_url}{params}")
    cursor.execute("select 1")


def test_cursor_skip_parse(
    mock_query: Callable,
    cursor: Cursor,
):
    """Cursor doesn't process a query if skip_parsing is provided."""
    mock_query()

    with patch("firebolt.db.cursor.split_format_sql") as split_format_sql_mock:
        cursor.execute("non-an-actual-sql")
        split_format_sql_mock.assert_called_once()

    with patch("firebolt.db.cursor.split_format_sql") as split_format_sql_mock:
        cursor.execute("non-an-actual-sql", skip_parsing=True)
        split_format_sql_mock.assert_not_called()


def test_cursor_server_side_async_execute(
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
            lambda: cursor.executemany("select * from t", [], async_execution=True),
            "server-side asynchronous executemany()",
        ),
    ):

        # Query with json output
        httpx_mock.add_callback(
            server_side_async_id_callback, url=query_with_params_url
        )

        assert (
            query() == server_side_async_id
        ), f"Invalid query id returned for {message}."
        assert (
            cursor.rowcount == -1
        ), f"Invalid rowcount value for insert using {message}."
        assert (
            cursor.description is None
        ), f"Invalid description for insert using {message}."


def test_cursor_server_side_async_cancel(
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
    cursor.cancel(server_side_async_id)

    cursor.close()
    with raises(CursorClosedError):
        cursor.cancel(server_side_async_id)


def test_cursor_server_side_async_get_status_completed(
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
    status = cursor.get_status(server_side_async_id)
    assert status == QueryStatus.ENDED_SUCCESSFULLY


def test_cursor_server_side_async_get_status_not_yet_available(
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
    status = cursor.get_status(server_side_async_id)
    assert status == QueryStatus.NOT_READY


def test_cursor_server_side_async_get_status_error(
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
        cursor.get_status(server_side_async_id)

        assert cursor._state == CursorState.ERROR
        assert (
            str(excinfo.value)
            == f"Asynchronous query {server_side_async_id} status check failed."
        ), f"Invalid get_status error message."


def test_cursor_iterate(
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    httpx_mock.add_callback(query_callback, url=query_url)

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
