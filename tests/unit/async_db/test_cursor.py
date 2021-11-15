from inspect import cleandoc
from typing import Callable, Dict, List

from httpx import HTTPStatusError, StreamError, codes
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db import Cursor
from firebolt.async_db._types import Column
from firebolt.async_db.cursor import ColType, CursorState
from firebolt.common.exception import (
    CursorClosedError,
    OperationalError,
    QueryNotRunError,
)


@mark.asyncio
async def test_cursor_state(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
):
    """Cursor state changes depending on the operations performed with it."""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    assert cursor._state == CursorState.NONE

    await cursor.execute("select")
    assert cursor._state == CursorState.DONE

    cursor._reset()
    assert cursor._state == CursorState.NONE

    cursor.close()
    assert cursor._state == CursorState.CLOSED


@mark.asyncio
async def test_closed_cursor(cursor: Cursor):
    """Most of cursor methods are unavailable for closed cursor."""
    fields = ("description", "rowcount")
    async_methods = (
        ("execute", (cursor,)),
        ("executemany", (cursor, [])),
        ("fetchone", ()),
        ("fetchmany", ()),
        ("fetchall", ()),
    )
    methods = (
        "setinputsizes",
        "setoutputsize",
    )

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


@mark.asyncio
async def test_cursor_no_query(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
):
    """Some of cursor methods are unavailable until a query is run."""
    async_methods = (
        "fetchone",
        "fetchmany",
        "fetchall",
    )

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    for amethod in async_methods:
        with raises(QueryNotRunError):
            await getattr(cursor, amethod)()

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


@mark.asyncio
async def test_cursor_execute(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    insert_query_callback: Callable,
    query_url: str,
    cursor: Cursor,
    python_query_description: List[Column],
    python_query_data: List[List[ColType]],
):
    """Cursor is able to execute query, all fields are populated properly."""

    for query in (
        lambda: cursor.execute("select *"),
        lambda: cursor.executemany("select *", [None, None]),
    ):
        # Query with json output
        httpx_mock.add_callback(auth_callback, url=auth_url)
        httpx_mock.add_callback(query_callback, url=query_url)
        assert await query() == len(python_query_data), "Invalid row count returned"
        assert cursor.rowcount == len(python_query_data), "Invalid rowcount value"
        for i, (desc, exp) in enumerate(
            zip(cursor.description, python_query_description)
        ):
            assert desc == exp, f"Invalid column description at position {i}"

        for i in range(cursor.rowcount):
            assert (
                await cursor.fetchone() == python_query_data[i]
            ), f"Invalid data row at position {i}"

        assert (
            await cursor.fetchone() is None
        ), "Non-empty fetchone after all data received"

        httpx_mock.reset(True)

        # Query with empty output
        httpx_mock.add_callback(auth_callback, url=auth_url)
        httpx_mock.add_callback(insert_query_callback, url=query_url)
        assert await query() == -1, "Invalid row count for insert query"
        assert cursor.rowcount == -1, "Invalid rowcount value for insert query"
        assert cursor.description is None, "Invalid description for insert query"
        assert await cursor.fetchone() is None, "Non-empty fetchone for insert query"


@mark.asyncio
async def test_cursor_execute_error(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_url: str,
    cursor: Cursor,
):
    """Cursor handles all types of errors properly."""
    for query in (
        lambda: cursor.execute("select *"),
        lambda: cursor.executemany("select *", [None, None]),
    ):
        httpx_mock.add_callback(auth_callback, url=auth_url)

        # Internal httpx error
        def http_error(**kwargs):
            raise StreamError("httpx error")

        httpx_mock.add_callback(http_error, url=query_url)
        with raises(StreamError) as excinfo:
            await query()

        assert str(excinfo.value) == "httpx error", "Invalid query error message"

        # HTTP error
        httpx_mock.add_response(status_code=codes.BAD_REQUEST, url=query_url)
        with raises(HTTPStatusError) as excinfo:
            await query()

        errmsg = str(excinfo.value)
        assert "Bad Request" in errmsg, "Invalid query error message"

        # Database query error
        httpx_mock.add_response(
            status_code=codes.INTERNAL_SERVER_ERROR,
            data="Query error message",
            url=query_url,
        )
        with raises(OperationalError) as excinfo:
            await query()

        assert (
            str(excinfo.value) == "Error executing query:\nQuery error message"
        ), "Invalid authentication error message"
        httpx_mock.reset(True)


@mark.asyncio
async def test_cursor_fetchone(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
):
    """cursor fetchone fetches single row in correct order, if no rows returns None."""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    await cursor.execute("sql")

    assert (await cursor.fetchone())[0] == 0, "Invalid rows order returned by fetchone"
    assert (await cursor.fetchone())[0] == 1, "Invalid rows order returned by fetchone"

    assert (
        len(await cursor.fetchall()) == cursor.rowcount - 2
    ), "Invalid row number returned by fetchall"

    assert (
        await cursor.fetchone() is None
    ), "fetchone should return None when no rows left to fetch"


@mark.asyncio
async def test_cursor_fetchmany(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
):
    cleandoc(
        """
        Cursor's fetchmany fetches the provided amount of rows, or arraysize by
        default. If not enough rows left, returns less or None if there are no rows.
        """
    )
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

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
    ), "fetchmany should return empty result set when no rows left to fetch"


@mark.asyncio
async def test_cursor_fetchall(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    cursor: Cursor,
):
    """cursor fetchall fetches all rows that left after last query."""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

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
    ), "fetchmany should return empty result set when no rows left to fetch"


# This tests a temporary functionality, needs to be removed when the
# functionality is removed
@mark.asyncio
async def test_set_parameters(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_with_params_url: str,
    query_with_params_callback: Callable,
    cursor: Cursor,
    set_params: Dict,
):
    """Cursor passes provided set parameters to engine"""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_with_params_callback, url=query_with_params_url)
    await cursor.execute("select 1", set_parameters=set_params)
