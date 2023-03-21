import gc
import warnings
from typing import Callable, List

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises, warns
from pytest_httpx import HTTPXMock

from firebolt.async_db._types import ColType
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.common.settings import Settings
from firebolt.db import Connection, connect
from firebolt.utils.exception import (
    ConfigurationError,
    ConnectionClosedError,
    InterfaceError,
)
from firebolt.utils.token_storage import TokenSecureStorage


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
    c1, c2 = connection.cursor(), connection.cursor()
    assert (
        len(connection._cursors) == 2
    ), "Invalid number of cursors stored in connection"

    connection.close()
    assert connection.closed, "Connection was not closed on close"
    assert c1.closed, "Cursor was not closed on connection close"
    assert c2.closed, "Cursor was not closed on connection close"
    assert len(connection._cursors) == 0, "Cursors left in connection after close"


def test_cursor_initialized(
    settings: Settings,
    db_name: str,
    mock_connection_flow: Callable,
    mock_query: Callable,
    account_name: str,
    python_query_data: List[List[ColType]],
    auth: Auth,
    engine_name: str,
) -> None:
    """Connection initialized its cursors properly."""
    mock_connection_flow()
    mock_query()

    with connect(
        engine_name=engine_name,
        database=db_name,
        api_endpoint=settings.server,
        auth=auth,
        account_name=account_name,
    ) as connection:

        cursor = connection.cursor()
        assert cursor.connection == connection, "Invalid cursor connection attribute"
        assert cursor._client == connection._client, "Invalid cursor _client attribute"

        assert cursor.execute("select*") == len(python_query_data)

        cursor.close()
        assert (
            cursor not in connection._cursors
        ), "Cursor wasn't removed from connection after close"


def test_connect_empty_parameters():
    with raises(ConfigurationError):
        with connect(engine_name="engine_name"):
            pass


def test_connect_engine_name(
    settings: Settings,
    db_name: str,
    httpx_mock: HTTPXMock,
    mock_connection_flow: Callable,
    mock_query: Callable,
    account_name: str,
    engine_name: str,
    python_query_data: List[List[ColType]],
    auth: Auth,
    system_engine_query_url: str,
    get_engine_url_not_running_callback: Callable,
    get_engine_url_invalid_db_callback: Callable,
    auth_url: str,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    get_engine_url_callback: Callable,
):
    """connect properly handles engine_name"""
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(get_system_engine_callback, url=get_system_engine_url)

    mock_query()

    for callback in (
        get_engine_url_invalid_db_callback,
        get_engine_url_not_running_callback,
    ):
        httpx_mock.add_callback(callback, url=system_engine_query_url)
        with raises(InterfaceError):
            connect(
                database="db",
                auth=auth,
                engine_name=engine_name,
                account_name=account_name,
                api_endpoint=settings.server,
            )

    httpx_mock.add_callback(get_engine_url_callback, url=system_engine_query_url)

    with connect(
        engine_name=engine_name,
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=settings.server,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connect_default_engine(
    settings: Settings,
    db_name: str,
    mock_query: Callable,
    httpx_mock: HTTPXMock,
    auth_url: str,
    check_credentials_callback: Callable,
    database_id: str,
    python_query_data: List[List[ColType]],
    account_id: str,
    auth: Auth,
    account_name: str,
    system_engine_query_url: str,
    get_default_db_engine_callback: Callable,
    get_default_db_engine_not_running_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
):
    mock_query()
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(get_system_engine_callback, url=get_system_engine_url)
    httpx_mock.add_callback(
        get_default_db_engine_not_running_callback, url=system_engine_query_url
    )
    with raises(InterfaceError):
        with connect(
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=settings.server,
        ) as connection:
            connection.cursor().execute("select*")

    httpx_mock.add_callback(get_default_db_engine_callback, url=system_engine_query_url)

    with connect(
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=settings.server,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connection_unclosed_warnings():
    c = Connection("", "", None, "")
    with warns(UserWarning) as winfo:
        del c
        gc.collect()

    assert any(
        "Unclosed" in str(warning.message) for warning in winfo.list
    ), "Invalid unclosed connection warning"


def test_connection_no_warnings():
    c = Connection("", "", None, "")
    c.close()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        del c
        gc.collect()


def test_connection_commit(connection: Connection):
    # nothing happens
    connection.commit()

    connection.close()
    with raises(ConnectionClosedError):
        connection.commit()


@mark.nofakefs
def test_connection_token_caching(
    settings: Settings,
    db_name: str,
    mock_connection_flow: Callable,
    mock_query: Callable,
    python_query_data: List[List[ColType]],
    access_token: str,
    client_id: str,
    client_secret: str,
    engine_name: str,
    account_name: str,
) -> None:
    mock_connection_flow()
    mock_query()

    with Patcher():
        with connect(
            database=db_name,
            auth=ClientCredentials(client_id, client_secret, use_token_cache=True),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=settings.server,
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
            api_endpoint=settings.server,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=client_id, password=client_secret)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"
