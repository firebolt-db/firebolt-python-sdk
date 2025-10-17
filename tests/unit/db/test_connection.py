import gc
import warnings
from typing import Callable, Generator, List, Optional, Tuple
from unittest.mock import ANY as AnyValue
from unittest.mock import MagicMock, patch

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises, warns
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth, ClientCredentials
from firebolt.client.client import ClientV2
from firebolt.common._types import ColType
from firebolt.common.base_connection import (
    TRANSACTION_ID_PARAMETER,
    TRANSACTION_SEQUENCE_ID_PARAMETER,
)
from firebolt.db import Connection, connect
from firebolt.db.cursor import CursorV2
from firebolt.utils.cache import _firebolt_cache
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    ConfigurationError,
    ConnectionClosedError,
    FireboltError,
    NotSupportedError,
)
from firebolt.utils.token_storage import TokenSecureStorage


def test_connection_attributes(connection: Connection) -> None:
    with raises(AttributeError):
        connection.not_a_cursor()

    with raises(AttributeError):
        connection.not_a_database


def test_closed_connection(connection: Connection) -> None:
    """Connection methods are unavailable for closed connection."""
    connection.close()

    with raises(ConnectionClosedError):
        connection.cursor()

    with raises(ConnectionClosedError):
        with connection:
            pass

    connection.close()


def test_cursors_closed_on_close(connection: Connection) -> None:
    """Connection closes all its cursors on close."""
    assert connection.closed == False, "Initial state of connection is incorrect"
    c1, c2 = connection.cursor(), connection.cursor()
    assert (
        len(connection._cursors) == 2
    ), "Invalid number of cursors stored in connection"

    connection.close()
    assert connection.closed == True, "Connection was not closed on close"
    assert c1.closed, "Cursor was not closed on connection close"
    assert c2.closed, "Cursor was not closed on connection close"
    assert len(connection._cursors) == 0, "Cursors left in connection after close"


def test_cursor_initialized(
    mock_query: Callable,
    connection: Connection,
    python_query_data: List[List[ColType]],
) -> None:
    """Connection initialized its cursors properly."""
    mock_query()

    cursor = connection.cursor()
    assert cursor.connection == connection, "Invalid cursor connection attribute"
    assert (
        cursor._client.base_url == connection._client.base_url
    ), "Invalid cursor client base_url"

    assert cursor.execute("select *") == len(python_query_data)

    cursor.close()
    assert (
        cursor not in connection._cursors
    ), "Cursor wasn't removed from connection after close"


def test_connect_empty_parameters():
    with raises(ConfigurationError):
        with connect(engine_name="engine_name"):
            pass


def test_connect(
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

    with connect(
        engine_name=engine_name,
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert connection.cursor().execute("select *") == len(python_query_data)


def test_connect_database_failed(
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
        with connect(
            database=db_name,
            auth=auth,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ):
            pass

    # Account id endpoint was not used since we didn't get to that point
    httpx_mock.reset()


def test_connect_engine_failed(
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
        with connect(
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
def test_connect_system_engine_caching(
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
        with connect(
            database=db_name,
            engine_name=engine_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            disable_cache=not cache_enabled,
        ) as connection:
            connection.cursor().execute("select*")

    if cache_enabled:
        assert system_engine_call_counter == 1, "System engine URL was not cached"
    else:
        assert system_engine_call_counter != 1, "System engine URL was cached"


def test_connect_system_engine_404(
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
        with connect(
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("select*")


def test_connection_unclosed_warnings(auth: Auth):
    c = Connection("", "", auth, "", None)
    with warns(UserWarning) as winfo:
        # Can't guarantee `del c` triggers garbage collection
        c.__del__()

    assert any(
        "Unclosed" in str(warning.message) for warning in winfo.list
    ), "Invalid unclosed connection warning"


def test_connection_no_warnings(client: ClientV2):
    c = Connection("", "", client, CursorV2, "")
    c.close()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        del c
        gc.collect()


def test_connection_commit(connection: Connection):
    # commit with no transaction should raise NotSupportedError
    with raises(NotSupportedError):
        connection.commit()

    connection.close()
    with raises(ConnectionClosedError):
        connection.commit()


@mark.nofakefs
def test_connection_token_caching(
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
        with connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    with Patcher():
        with connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=True),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        with connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=False),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"


def test_connect_with_user_agent(
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

        with connect(
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
            connection.cursor().execute("select*")
        ut.assert_called_with([("DriverA", "1.1")], [("MyConnector", "1.0")], AnyValue)


def test_connect_no_user_agent(
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

        with connect(
            auth=auth,
            database=db_name,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("select*")
        ut.assert_called_with([], [], AnyValue)


def test_connect_caching_headers(
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
    def do_connect():
        with connect(
            auth=auth,
            database=db_name,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("select*")

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
        with patch("firebolt.db.connection.uuid4") as uuid4:
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

            do_connect()
            ut.assert_called_with(AnyValue, AnyValue, [("connId", mock_id)])

            # Second call should use cached connection info
            do_connect()
            ut.assert_called_with(
                AnyValue,
                AnyValue,
                [("connId", mock_id2), ("cachedConnId", f"{mock_id}-memory")],
            )
            _firebolt_cache.clear()

            # Third call should have a new connection id
            do_connect()
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
def test_is_async_query_running_success(
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

    with connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert connection.is_async_query_running("token") is expected_running
        assert connection.is_async_query_successful("token") is expected_success


def test_async_query_status_unexpected_result(
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

    with connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(FireboltError):
            connection.is_async_query_running("token")
        with raises(FireboltError):
            connection.is_async_query_successful("token")


def test_async_query_status_no_id_or_status(
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
        with connect(
            database=db_name,
            auth=auth,
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            with raises(FireboltError):
                connection.is_async_query_running("token")
            with raises(FireboltError):
                connection.is_async_query_successful("token")


def test_async_query_cancellation(
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
    """Test is_async_query_running method"""
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

    with connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        connection.cancel_async_query("token")


def test_get_async_query_info(
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

    with connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        result = connection.get_async_query_info("token")

        # Verify we got a list with one AsyncQueryInfo object
        assert len(result) == 1
        expected_server_status = async_query_data[0][5]
        assert result[0].status == expected_server_status

        # Verify query_id matches the expected value from the data
        expected_query_id = async_query_data[0][7]  # Index of query_id in data
        assert result[0].query_id == expected_query_id


def test_multiple_results_for_async_token(
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

    with connect(
        database=db_name,
        auth=auth,
        engine_name=engine_name,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(NotImplementedError):
            connection.is_async_query_successful("token")
        with raises(NotImplementedError):
            connection.is_async_query_running("token")

        query_info = connection.get_async_query_info("token")
        assert len(query_info) == 2, "Expected two results for the same token"
        assert query_info[0].query_id == async_multiple_query_data[0][7]
        assert query_info[1].query_id == async_multiple_query_data[1][7]


# Transaction tests


def test_connection_autocommit_default(connection: Connection) -> None:
    """Test that connections default to autocommit mode."""
    assert connection.autocommit is True
    assert connection.in_transaction is False


@mark.parametrize("autocommit_value", [False, True])
def test_connection_autocommit_property(
    connection: Connection, autocommit_value: bool
) -> None:
    """Test autocommit property getter and setter."""
    # Set initial state
    connection.autocommit = autocommit_value
    assert connection.autocommit is autocommit_value

    # Set opposite state
    connection.autocommit = not autocommit_value
    assert connection.autocommit is (not autocommit_value)


def test_connection_autocommit_closed_error(connection: Connection) -> None:
    """Test that setting autocommit on closed connection raises error."""
    connection.close()

    with raises(
        ConnectionClosedError, match="Unable to set autocommit: Connection closed"
    ):
        connection.autocommit = False


@mark.parametrize("method_name", ["commit", "rollback"])
def test_connection_transaction_methods_no_transaction(
    connection: Connection, method_name: str
) -> None:
    """Test that commit/rollback raise NotSupportedError when no transaction is active."""
    method = getattr(connection, method_name)

    with raises(NotSupportedError, match=f"No active transaction to {method_name}"):
        method()


@mark.parametrize("method_name", ["commit", "rollback"])
def test_connection_transaction_methods_closed_error(
    connection: Connection, method_name: str
) -> None:
    """Test that commit/rollback on closed connection raise ConnectionClosedError."""
    connection.close()
    method = getattr(connection, method_name)

    with raises(
        ConnectionClosedError, match=f"Unable to {method_name}: Connection closed"
    ):
        method()


@mark.parametrize(
    "method_name,expected_statement",
    [
        ("commit", "COMMIT"),
        ("rollback", "ROLLBACK"),
    ],
)
@patch.object(Connection, "_execute_transaction_statement")
def test_connection_transaction_methods_with_transaction(
    mock_execute: MagicMock,
    connection: Connection,
    method_name: str,
    expected_statement: str,
) -> None:
    """Test that commit/rollback execute correct statements when transaction is active."""
    # Simulate being in transaction
    connection._set_transaction_state(True)
    method = getattr(connection, method_name)

    method()

    mock_execute.assert_called_once_with(expected_statement)


def test_autocommit_false_defers_transaction(connection: Connection) -> None:
    """Test that setting autocommit=False defers beginning a transaction."""
    connection.autocommit = False

    # Should not be in transaction yet - transaction is deferred until first statement
    assert connection.in_transaction is False


@patch.object(Connection, "commit")
def test_autocommit_true_commits_active_transaction(
    mock_commit: MagicMock, connection: Connection
) -> None:
    """Test that setting autocommit=True commits active transaction."""
    # Simulate being in transaction with autocommit off
    connection._autocommit = False
    connection._set_transaction_state(True)

    connection.autocommit = True

    mock_commit.assert_called_once()


def test_transaction_state_sync_to_cursors(connection: Connection) -> None:
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


def test_transaction_id_handling(connection: Connection) -> None:
    """Test transaction ID parameter handling."""
    cursor = connection.cursor()

    # Initially no transaction
    assert connection.in_transaction is False

    # Simulate receiving transaction_id parameter
    connection._on_transaction_parameter_received(TRANSACTION_ID_PARAMETER, "tx_123")

    assert connection.in_transaction is True
    assert cursor._in_transaction is True

    # Simulate transaction_id removal
    connection._on_transaction_parameter_removed(TRANSACTION_ID_PARAMETER)

    assert connection.in_transaction is False
    assert cursor._in_transaction is False


def test_transaction_sequence_id_handling(connection: Connection) -> None:
    """Test transaction sequence ID parameter handling."""
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Initially no transaction_sequence_id
    assert connection.transaction_sequence_id is None
    assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor1._set_parameters
    assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor2._set_parameters

    # Simulate receiving transaction_sequence_id parameter
    connection._on_transaction_parameter_received(
        TRANSACTION_SEQUENCE_ID_PARAMETER, "seq_456"
    )

    assert connection.transaction_sequence_id == "seq_456"
    assert cursor1._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] == "seq_456"
    assert cursor2._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] == "seq_456"

    # Simulate transaction_sequence_id removal
    connection._on_transaction_parameter_removed(TRANSACTION_SEQUENCE_ID_PARAMETER)

    assert connection.transaction_sequence_id is None
    assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor1._set_parameters
    assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor2._set_parameters


def test_transaction_sequence_id_inheritance_on_cursor_creation(
    connection: Connection,
) -> None:
    """Test that new cursors inherit existing transaction_sequence_id from connection."""
    # Set transaction_sequence_id on connection
    connection._on_transaction_parameter_received(
        TRANSACTION_SEQUENCE_ID_PARAMETER, "seq_789"
    )

    # Create new cursor - should inherit transaction_sequence_id
    cursor = connection.cursor()

    assert cursor._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] == "seq_789"


def test_multi_cursor_transaction_sequence_id_sync(connection: Connection) -> None:
    """Test transaction_sequence_id synchronization across multiple cursors."""
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()
    cursor3 = connection.cursor()

    # All cursors should initially have no transaction_sequence_id
    for cursor in [cursor1, cursor2, cursor3]:
        assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor._set_parameters

    # Set transaction_sequence_id - should propagate to all cursors
    connection._on_transaction_parameter_received(
        TRANSACTION_SEQUENCE_ID_PARAMETER, "seq_multi_123"
    )

    for cursor in [cursor1, cursor2, cursor3]:
        assert (
            cursor._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] == "seq_multi_123"
        )

    # Remove transaction_sequence_id - should be removed from all cursors
    connection._on_transaction_parameter_removed(TRANSACTION_SEQUENCE_ID_PARAMETER)

    for cursor in [cursor1, cursor2, cursor3]:
        assert TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor._set_parameters
