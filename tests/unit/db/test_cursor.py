from typing import Any, Callable, Dict, List
from unittest.mock import patch

from httpx import URL, HTTPStatusError, Request, StreamError, codes
from pytest import LogCaptureFixture, mark, raises
from pytest_httpx import HTTPXMock

from firebolt.db import Cursor
from firebolt.db.cursor import ColType, Column, CursorState
from firebolt.utils.exception import (
    ConfigurationError,
    CursorClosedError,
    DataError,
    OperationalError,
    ProgrammingError,
    QueryNotRunError,
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

        httpx_mock.add_callback(http_error, url=query_url)
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


def test_server_side_header_database(
    httpx_mock: HTTPXMock,
    query_callback_with_headers: Callable,
    query_url: str,
    query_url_updated: str,
    db_name: str,
    db_name_updated: str,
    cursor: Cursor,
):
    httpx_mock.add_callback(query_callback_with_headers, url=query_url)
    assert cursor.database == db_name
    cursor.execute(f"USE DATABASE = '{db_name_updated}'")
    assert cursor.database == db_name_updated

    httpx_mock.reset(True)
    # Check updated database is used in the next query
    httpx_mock.add_callback(query_callback_with_headers, url=query_url_updated)
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

    httpx_mock.add_callback(query_callback_with_headers, url=query_url)
    assert cursor.engine_url == "https://" + query_url.host
    cursor.execute("USE ENGINE = 'my_dummy_engine'")
    assert cursor.engine_url == f"https://{query_updated_url}"

    httpx_mock.reset(True)
    # Check updated engine is used in the next query
    new_url = query_url.copy_with(host=query_updated_url)
    httpx_mock.add_callback(query_callback_with_headers, url=new_url)
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

    httpx_mock.add_callback(query_callback_with_headers, url=query_url)
    assert cursor.engine_url == "https://" + query_url.host
    cursor.execute("USE ENGINE = 'my_dummy_engine'")
    assert cursor.engine_url == f"https://{query_updated_url}"
    assert cursor._set_parameters == {"param1": "1", "param2": "2"}
    assert list(cursor.parameters.keys()) == ["database", "engine"]
    assert cursor.engine_name == "my_dummy_engine"

    httpx_mock.reset(True)
    # Check new parameters are used in the URL
    new_url = query_url.copy_with(host=query_updated_url).copy_merge_params(
        {"param1": "1", "param2": "2", "engine": "my_dummy_engine"}
    )
    httpx_mock.add_callback(query_callback_with_headers, url=new_url)
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

    httpx_mock.add_callback(select_one_query_callback, url=f"{set_query_url}&a=b")

    assert len(cursor._set_parameters) == 0

    cursor.execute("set a = b")
    assert (
        len(cursor._set_parameters) == 1
        and "a" in cursor._set_parameters
        and cursor._set_parameters["a"] == "b"
    )

    httpx_mock.reset(True)
    httpx_mock.add_callback(
        query_callback_with_headers,
        url=f"{set_query_url}&a=b&output_format=JSON_Compact",
    )
    cursor.execute("SELECT 1")
    assert len(cursor._set_parameters) == 0
    assert bool(cursor.engine_url) is True, "engine url is not set"
    assert bool(cursor.database) is True, "database is not set"
