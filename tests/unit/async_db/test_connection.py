from typing import Callable, List
from unittest.mock import patch

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db.connection import Connection, connect
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.common._types import ColType
from firebolt.common.cache import _firebolt_system_engine_cache
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    ConfigurationError,
    ConnectionClosedError,
    FireboltError,
)
from firebolt.utils.token_storage import TokenSecureStorage


@mark.skip("__slots__ is broken on Connection class")
async def test_connection_attributes(connection: Connection) -> None:
    """Test that no unexpected values can be set. Governed by __slots__"""
    with raises(AttributeError):
        connection.not_a_database = "dummy"


async def test_closed_connection(connection: Connection) -> None:
    """Connection methods are unavailable for closed connection."""
    await connection.aclose()

    with raises(ConnectionClosedError):
        connection.cursor()

    with raises(ConnectionClosedError):
        async with connection:
            pass

    await connection.aclose()


async def test_cursors_closed_on_close(connection: Connection) -> None:
    """Connection closes all its cursors on close."""
    assert connection.closed == False, "Initial state of connection is incorrect"
    c1, c2 = connection.cursor(), connection.cursor()
    assert (
        len(connection._cursors) == 2
    ), "Invalid number of cursors stored in connection."

    await connection.aclose()
    assert connection.closed == True, "Connection was not closed on close."
    assert c1.closed, "Cursor was not closed on connection close."
    assert c2.closed, "Cursor was not closed on connection close."
    assert len(connection._cursors) == 0, "Cursors left in connection after close."
    await connection.aclose()


async def test_cursor_initialized(
    mock_query: Callable,
    connection: Connection,
    python_query_data: List[List[ColType]],
) -> None:
    """Connection initialized its cursors properly."""
    mock_query()

    cursor = connection.cursor()
    assert cursor.connection == connection, "Invalid cursor connection attribute."
    assert (
        cursor._client.base_url == connection._client.base_url
    ), "Invalid cursor _client attribute"

    assert await cursor.execute("select*") == len(python_query_data)

    cursor.close()
    assert (
        cursor not in connection._cursors
    ), "Cursor wasn't removed from connection after close."


async def test_connect_empty_parameters():
    with raises(ConfigurationError):
        async with await connect(engine_name="engine_name"):
            pass


async def test_connect(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    python_query_data: List[List[ColType]],
    mock_connection_flow: Callable,
    mock_query: Callable,
):
    """connect properly handles engine_name"""
    mock_connection_flow()
    mock_query()

    async with await connect(
        engine_name=engine_name,
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert await connection.cursor().execute("select *") == len(python_query_data)


async def test_connect_database_failed(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    python_query_data: List[List[ColType]],
    httpx_mock: HTTPXMock,
    system_engine_no_db_query_url: str,
    use_database_failed_callback: Callable,
    mock_system_engine_connection_flow: Callable,
    mock_query: Callable,
):
    """connect properly handles use database errors"""
    mock_system_engine_connection_flow()

    httpx_mock.add_callback(
        use_database_failed_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )
    with raises(FireboltError):
        async with await connect(
            database=db_name,
            auth=auth,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ):
            pass

    # Account id endpoint was not used since we didn't get to that point
    httpx_mock.reset(False)


async def test_connect_engine_failed(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    python_query_data: List[List[ColType]],
    httpx_mock: HTTPXMock,
    system_engine_no_db_query_url: str,
    use_database_callback: Callable,
    system_engine_query_url: str,
    use_engine_failed_callback: Callable,
    mock_system_engine_connection_flow: Callable,
    mock_query: Callable,
):
    """connect properly handles use engine errors"""
    mock_system_engine_connection_flow()

    httpx_mock.add_callback(
        use_database_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )

    httpx_mock.add_callback(
        use_engine_failed_callback,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
    )
    with raises(FireboltError):
        async with await connect(
            database=db_name,
            auth=auth,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ):
            pass

    # Account id endpoint was not used since we didn't get to that point
    httpx_mock.reset(False)


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_caching(
    db_name: str,
    engine_name: str,
    auth_url: str,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    cache_enabled: bool,
):
    system_engine_call_counter = 0

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(system_engine_callback_counter, url=get_system_engine_url)
    httpx_mock.add_callback(
        use_database_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )

    httpx_mock.add_callback(
        use_engine_callback,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
    )
    httpx_mock.add_callback(query_callback, url=query_url)

    for _ in range(3):
        async with await connect(
            database=db_name,
            engine_name=engine_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            disable_cache=not cache_enabled,
        ) as connection:
            await connection.cursor().execute("select*")

    if cache_enabled:
        assert system_engine_call_counter == 1, "System engine URL was not cached"
    else:
        assert system_engine_call_counter != 1, "System engine URL was cached"

    # Reset caches for the next test iteration
    _firebolt_system_engine_cache.enable()


async def test_connect_system_engine_404(
    db_name: str,
    auth_url: str,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_404_callback: Callable,
):
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(get_system_engine_404_callback, url=get_system_engine_url)
    with raises(AccountNotFoundOrNoAccessError):
        async with await connect(
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")


async def test_connection_commit(connection: Connection):
    # nothing happens
    connection.commit()

    await connection.aclose()
    with raises(ConnectionClosedError):
        connection.commit()


@mark.nofakefs
async def test_connection_token_caching(
    db_name: str,
    api_endpoint: str,
    access_token: str,
    client_id: str,
    client_secret: str,
    engine_name: str,
    account_name: str,
    python_query_data: List[List[ColType]],
    mock_connection_flow: Callable,
    mock_query: Callable,
) -> None:
    mock_connection_flow()
    mock_query()

    # Using caching by default
    with Patcher():
        async with await connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=True),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert await connection.cursor().execute("select*") == len(
                python_query_data
            )
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    with Patcher():
        async with await connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=True),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert await connection.cursor().execute("select*") == len(
                python_query_data
            )
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        async with await connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=False),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert await connection.cursor().execute("select*") == len(
                python_query_data
            )
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"


async def test_connect_with_user_agent(
    engine_name: str,
    account_name: str,
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    access_token: str,
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    mock_connection_flow: Callable,
) -> None:
    with patch("firebolt.async_db.connection.get_user_agent_header") as ut:
        ut.return_value = "MyConnector/1.0 DriverA/1.1"
        mock_connection_flow()
        httpx_mock.add_callback(
            query_callback,
            url=query_url,
            match_headers={"User-Agent": "MyConnector/1.0 DriverA/1.1"},
        )

        async with await connect(
            auth=auth,
            database=db_name,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
            additional_parameters={
                "user_clients": [("MyConnector", "1.0")],
                "user_drivers": [("DriverA", "1.1")],
            },
        ) as connection:
            await connection.cursor().execute("select*")
        ut.assert_called_with([("DriverA", "1.1")], [("MyConnector", "1.0")])


async def test_connect_no_user_agent(
    engine_name: str,
    account_name: str,
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    access_token: str,
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    mock_connection_flow: Callable,
) -> None:
    with patch("firebolt.async_db.connection.get_user_agent_header") as ut:
        ut.return_value = "Python/3.0"
        mock_connection_flow()
        httpx_mock.add_callback(
            query_callback, url=query_url, match_headers={"User-Agent": "Python/3.0"}
        )

        async with await connect(
            auth=auth,
            database=db_name,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")
        ut.assert_called_with([], [])
