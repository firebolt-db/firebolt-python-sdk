from typing import Callable, List

from httpx import codes
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db import Connection, connect
from firebolt.async_db._types import ColType
from firebolt.common.exception import ConnectionClosedError, InterfaceError
from firebolt.common.settings import Settings
from firebolt.common.urls import ENGINE_BY_NAME_URL


@mark.asyncio
async def test_closed_connection(connection: Connection) -> None:
    """Connection methods are unavailable for closed connection."""
    await connection.aclose()

    with raises(ConnectionClosedError):
        connection.cursor()

    with raises(ConnectionClosedError):
        async with connection:
            pass

    await connection.aclose()


@mark.asyncio
async def test_cursors_closed_on_close(connection: Connection) -> None:
    """Connection closes all it's cursors on close."""
    c1, c2 = connection.cursor(), connection.cursor()
    assert (
        len(connection._cursors) == 2
    ), "Invalid number of cursors stored in connection"

    await connection.aclose()
    assert connection.closed, "Connection was not closed on close"
    assert c1.closed, "Cursor was not closed on connection close"
    assert c2.closed, "Cursor was not closed on connection close"
    assert len(connection._cursors) == 0, "Cursors left in connection after close"
    await connection.aclose()


@mark.asyncio
async def test_cursor_initialized(
    settings: Settings,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
) -> None:
    """Connection initialised it's cursors propperly"""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    for url in (settings.server, f"https://{settings.server}"):
        async with (
            await connect(
                engine_url=url,
                database=db_name,
                username="u",
                password="p",
                api_endpoint=settings.server,
            )
        ) as connection:
            cursor = connection.cursor()
            assert (
                cursor.connection == connection
            ), "Invalid cursor connection attribute"
            assert (
                cursor._client == connection._client
            ), "Invalid cursor _client attribute"

            assert await cursor.execute("select*") == len(python_query_data)

            cursor.close()
            assert (
                cursor not in connection._cursors
            ), "Cursor wasn't removed from connection after close"


@mark.asyncio
async def test_connect_empty_parameters():
    params = ("database", "username", "password")
    kwargs = {"engine_url": "engine_url", **{p: p for p in params}}

    for param in params:
        with raises(InterfaceError) as exc_info:
            kwargs = {
                "engine_url": "engine_url",
                **{p: p for p in params if p != param},
            }
            async with await connect(**kwargs):
                pass
        assert str(exc_info.value) == f"{param} is required to connect."


@mark.asyncio
async def test_connect_engine_name(
    settings: Settings,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    account_id_url: str,
    account_id_callback: Callable,
    engine_id: str,
    get_engine_url: str,
    get_engine_callback: Callable,
    get_providers_url: str,
    get_providers_callback: Callable,
    python_query_data: List[List[ColType]],
    account_id: str,
):
    """connect properly handles engine_name"""

    with raises(InterfaceError) as exc_info:
        async with await connect(
            engine_url="engine_url",
            engine_name="engine_name",
            database="db",
            username="username",
            password="password",
        ):
            pass
    assert str(exc_info.value).startswith(
        "Both engine_name and engine_url are provided"
    )

    with raises(InterfaceError) as exc_info:
        async with await connect(
            database="db",
            username="username",
            password="password",
        ):
            pass
    assert str(exc_info.value).startswith(
        "Neither engine_name nor engine_url are provided"
    )

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(get_engine_callback, url=get_engine_url)

    engine_name = settings.server.split(".")[0]

    # Mock engine id lookup by name
    httpx_mock.add_response(
        url=f"https://{settings.server}"
        + ENGINE_BY_NAME_URL
        + f"?engine_name={engine_name}",
        status_code=codes.OK,
        json={"engine_id": {"engine_id": engine_id}},
    )

    async with await connect(
        engine_name=engine_name,
        database=db_name,
        username="u",
        password="p",
        api_endpoint=settings.server,
    ) as connection:
        assert await connection.cursor().execute("select*") == len(python_query_data)
