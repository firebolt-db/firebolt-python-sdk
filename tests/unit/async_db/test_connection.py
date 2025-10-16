from typing import Callable, Generator, List, Optional, Tuple
from unittest.mock import ANY as AnyValue
from unittest.mock import MagicMock, patch

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db.connection import Connection, connect
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.common._types import ColType
from firebolt.common.base_connection import (
    TRANSACTION_COMMIT,
    TRANSACTION_ROLLBACK,
)
from firebolt.utils.cache import _firebolt_cache
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    ConfigurationError,
    ConnectionClosedError,
    FireboltError,
    NotSupportedError,
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
        is_reusable=True,
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
    httpx_mock.reset()


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_system_engine_caching(
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
    enable_cache: Generator,
    cache_enabled: bool,
):
    system_engine_call_counter = 0

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )
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
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

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
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_engine_failed_callback,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
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
    httpx_mock.reset()


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
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        get_system_engine_404_callback,
        url=get_system_engine_url,
        is_reusable=True,
    )
    with raises(AccountNotFoundOrNoAccessError):
        async with await connect(
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")


async def test_connection_commit(connection: Connection):
    # async connection commit() should raise NotSupportedError suggesting acommit()
    with raises(NotSupportedError, match="Use acommit\\(\\) for async connections"):
        connection.commit()

    await connection.aclose()
    with raises(NotSupportedError, match="Use acommit\\(\\) for async connections"):
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
    with patch("firebolt.common.base_connection.get_user_agent_header") as ut:
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
        ut.assert_called_with([("DriverA", "1.1")], [("MyConnector", "1.0")], AnyValue)


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
    with patch("firebolt.common.base_connection.get_user_agent_header") as ut:
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
        ut.assert_called_with([], [], AnyValue)


async def test_connect_caching_headers(
    engine_name: str,
    account_name: str,
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    httpx_mock: HTTPXMock,
    query_callback: Callable,
    query_url: str,
    mock_connection_flow: Callable,
    enable_cache: Generator,
) -> None:
    async def do_connect():
        async with await connect(
            auth=auth,
            database=db_name,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")

    _firebolt_cache.clear()
    mock_id = "12345"
    mock_id2 = "67890"
    mock_id3 = "54321"
    with patch("firebolt.common.base_connection.get_user_agent_header") as ut:
        ut.side_effect = [
            f"connId:{mock_id}",
            f"connId:{mock_id2}; cachedConnId:{mock_id}-memory",
            f"connId:{mock_id3}",
        ]
        with patch("firebolt.async_db.connection.uuid4") as uuid4:
            uuid4.side_effect = [
                MagicMock(hex=mock_id),
                MagicMock(hex=mock_id2),
                MagicMock(hex=mock_id3),
            ]
            mock_connection_flow()
            httpx_mock.add_callback(
                query_callback,
                url=query_url,
                match_headers={"User-Agent": f"connId:{mock_id}"},
            )
            httpx_mock.add_callback(
                query_callback,
                url=query_url,
                match_headers={
                    "User-Agent": f"connId:{mock_id2}; cachedConnId:{mock_id}-memory"
                },
            )
            httpx_mock.add_callback(
                query_callback,
                url=query_url,
                match_headers={"User-Agent": f"connId:{mock_id3}"},
            )

            await do_connect()
            ut.assert_called_with(AnyValue, AnyValue, [("connId", mock_id)])

            # Second call should use cached connection info
            await do_connect()
            ut.assert_called_with(
                AnyValue,
                AnyValue,
                [("connId", mock_id2), ("cachedConnId", f"{mock_id}-memory")],
            )
            _firebolt_cache.clear()

            # Third call should have a new connection id
            await do_connect()
            ut.assert_called_with(AnyValue, AnyValue, [("connId", mock_id3)])


@mark.parametrize(
    "server_status,expected_running,expected_success",
    [
        ("RUNNING", True, None),
        ("ENDED_SUCCESSFULLY", False, True),
        ("FAILED", False, False),
        ("CANCELLED", False, False),
    ],
)
async def test_is_async_query_running_success(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    async_query_callback_factory: Callable,
    async_query_data: List[List[ColType]],
    async_query_meta: List[Tuple[str, str]],
    mock_connection_flow: Callable,
    server_status: str,
    expected_running: bool,
    expected_success: Optional[bool],
):
    """Test is_async_query_running method"""
    mock_connection_flow()
    async_query_data[0][5] = server_status
    async_query_status_running_callback = async_query_callback_factory(
        async_query_data, async_query_meta
    )

    httpx_mock.add_callback(
        async_query_status_running_callback,
        url=query_url,
        match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
        is_reusable=True,
    )

    async with await connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert await connection.is_async_query_running("token") is expected_running
        assert await connection.is_async_query_successful("token") is expected_success


async def test_async_query_status_unexpected_result(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    async_query_callback_factory: Callable,
    async_query_meta: List[Tuple[str, str]],
    mock_connection_flow: Callable,
):
    """Test is_async_query_running method"""
    mock_connection_flow()
    async_query_status_running_callback = async_query_callback_factory(
        [], async_query_meta
    )

    httpx_mock.add_callback(
        async_query_status_running_callback,
        url=query_url,
        match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
        is_reusable=True,
    )

    async with await connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(FireboltError):
            await connection.is_async_query_running("token")
        with raises(FireboltError):
            await connection.is_async_query_successful("token")


async def test_async_query_status_no_id_or_status(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    async_query_callback_factory: Callable,
    async_query_meta: List[Tuple[str, str]],
    async_query_data: List[List[ColType]],
    mock_connection_flow: Callable,
):
    mock_connection_flow()
    data_no_query_id = async_query_data[0].copy()
    data_no_query_id[7] = ""
    data_no_query_status = async_query_data[0].copy()
    data_no_query_status[5] = ""
    for data_case in [data_no_query_id, data_no_query_status]:
        async_query_status_running_callback = async_query_callback_factory(
            [data_case], async_query_meta
        )
        httpx_mock.add_callback(
            async_query_status_running_callback,
            url=query_url,
            match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
            is_reusable=True,
        )
        async with await connect(
            database=db_name,
            auth=auth,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            with raises(FireboltError):
                await connection.is_async_query_running("token")
            with raises(FireboltError):
                await connection.is_async_query_successful("token")


async def test_async_query_cancellation(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    query_callback: Callable,
    async_query_callback_factory: Callable,
    async_query_data: List[List[ColType]],
    async_query_meta: List[Tuple[str, str]],
    mock_connection_flow: Callable,
):
    """Test async query cancellation"""
    mock_connection_flow()
    async_query_data[0][5] = "RUNNING"
    async_query_status_running_callback = async_query_callback_factory(
        async_query_data, async_query_meta
    )

    query_dict = dict(zip([m[0] for m in async_query_meta], async_query_data[0]))
    query_id = query_dict["query_id"]

    httpx_mock.add_callback(
        async_query_status_running_callback,
        url=query_url,
        match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
        is_reusable=True,
    )

    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        match_content=f"CANCEL QUERY WHERE query_id='{query_id}'".encode("utf-8"),
        is_reusable=True,
    )

    async with await connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        await connection.cancel_async_query("token")


async def test_get_async_query_info(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    async_query_callback_factory: Callable,
    async_query_data: List[List[ColType]],
    async_query_meta: List[Tuple[str, str]],
    mock_connection_flow: Callable,
):
    """Test get_async_query_info method"""
    mock_connection_flow()
    async_query_callback = async_query_callback_factory(
        async_query_data, async_query_meta
    )

    httpx_mock.add_callback(
        async_query_callback,
        url=query_url,
        match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
        is_reusable=True,
    )

    async with await connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        result = await connection.get_async_query_info("token")

        # Verify we got a list with one AsyncQueryInfo object
        assert len(result) == 1
        expected_server_status = async_query_data[0][5]
        assert result[0].status == expected_server_status

        # Verify query_id matches the expected value from the data
        expected_query_id = async_query_data[0][7]  # Index of query_id in data
        assert result[0].query_id == expected_query_id


async def test_multiple_results_for_async_token(
    db_name: str,
    account_name: str,
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    httpx_mock: HTTPXMock,
    query_url: str,
    async_query_callback_factory: Callable,
    async_multiple_query_data: List[List[ColType]],
    async_query_meta: List[Tuple[str, str]],
    mock_connection_flow: Callable,
):
    """
    Test get_async_query_info method with multiple results for the same token.
    This future-proofs the code against changes in the server response.
    """
    mock_connection_flow()
    async_query_callback = async_query_callback_factory(
        async_multiple_query_data, async_query_meta
    )

    httpx_mock.add_callback(
        async_query_callback,
        url=query_url,
        match_content="CALL fb_GetAsyncStatus('token')".encode("utf-8"),
        is_reusable=True,
    )

    async with await connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(NotImplementedError):
            await connection.is_async_query_successful("token")
        with raises(NotImplementedError):
            await connection.is_async_query_running("token")

        query_info = await connection.get_async_query_info("token")
        assert len(query_info) == 2, "Expected two results for the same token"
        assert query_info[0].query_id == async_multiple_query_data[0][7]
        assert query_info[1].query_id == async_multiple_query_data[1][7]


# Transaction tests


async def test_connection_autocommit_default(connection: Connection) -> None:
    """Test that connections default to autocommit mode."""
    assert connection.autocommit is True
    assert connection.in_transaction is False


@mark.parametrize("autocommit_value", [False, True])
async def test_connection_autocommit_property(
    connection: Connection, autocommit_value: bool
) -> None:
    """Test autocommit property getter and setter."""
    # Set initial state
    connection.autocommit = autocommit_value
    assert connection.autocommit is autocommit_value

    # Set opposite state
    connection.autocommit = not autocommit_value
    assert connection.autocommit is (not autocommit_value)


async def test_connection_autocommit_closed_error(connection: Connection) -> None:
    """Test that setting autocommit on closed connection raises error."""
    await connection.aclose()

    with raises(
        ConnectionClosedError, match="Unable to set autocommit: Connection closed"
    ):
        connection.autocommit = False


@mark.parametrize("method_name", ["commit", "rollback"])
async def test_connection_sync_transaction_methods_not_supported(
    connection: Connection, method_name: str
) -> None:
    """Test that sync commit/rollback methods are not supported for async connections."""
    method = getattr(connection, method_name)
    expected_async_method = f"a{method_name}"

    with raises(
        NotSupportedError,
        match=f"Use {expected_async_method}\\(\\) for async connections",
    ):
        method()


@mark.parametrize("method_name", ["commit", "rollback"])
async def test_connection_sync_transaction_methods_closed_error(
    connection: Connection, method_name: str
) -> None:
    """Test that sync commit/rollback on closed async connection raise NotSupportedError."""
    await connection.aclose()
    method = getattr(connection, method_name)
    expected_async_method = f"a{method_name}"

    with raises(
        NotSupportedError,
        match=f"Use {expected_async_method}\\(\\) for async connections",
    ):
        method()


@mark.parametrize(
    "method_name,expected_statement",
    [
        ("acommit", TRANSACTION_COMMIT),
        ("arollback", TRANSACTION_ROLLBACK),
    ],
)
@patch.object(Connection, "_execute_transaction_statement")
async def test_connection_async_transaction_methods_with_transaction(
    mock_execute: MagicMock,
    connection: Connection,
    method_name: str,
    expected_statement: str,
) -> None:
    """Test that acommit/arollback execute correct statements when transaction is active."""
    # Simulate being in transaction
    connection._set_transaction_state(True)
    method = getattr(connection, method_name)

    await method()

    mock_execute.assert_called_once_with(expected_statement)


async def test_autocommit_false_defers_transaction(connection: Connection) -> None:
    """Test that setting autocommit=False defers beginning a transaction."""
    connection.autocommit = False

    # Should not be in transaction yet - transaction is deferred until first statement
    assert connection.in_transaction is False


@patch.object(Connection, "commit")
async def test_autocommit_true_commits_active_transaction(
    mock_commit: MagicMock, connection: Connection
) -> None:
    """Test that setting autocommit=True commits active transaction."""
    # Simulate being in transaction with autocommit off
    connection._autocommit = False
    connection._set_transaction_state(True)

    connection.autocommit = True

    mock_commit.assert_called_once()


async def test_transaction_state_sync_to_cursors(connection: Connection) -> None:
    """Test that transaction state is synchronized to all cursors."""
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Initially no transaction
    assert cursor1._in_transaction is False
    assert cursor2._in_transaction is False

    # Simulate transaction start
    connection._set_transaction_state(True)

    assert cursor1._in_transaction is True
    assert cursor2._in_transaction is True

    # Simulate transaction end
    connection._set_transaction_state(False)

    assert cursor1._in_transaction is False
    assert cursor2._in_transaction is False


async def test_transaction_id_handling(connection: Connection) -> None:
    """Test transaction ID parameter handling."""
    cursor = connection.cursor()

    # Initially no transaction
    assert connection.in_transaction is False

    # Simulate receiving transaction_id parameter
    connection._on_transaction_id_received("tx_123")

    assert connection.in_transaction is True
    assert cursor._in_transaction is True

    # Simulate transaction_id removal
    connection._on_transaction_id_removed()

    assert connection.in_transaction is False
    assert cursor._in_transaction is False
