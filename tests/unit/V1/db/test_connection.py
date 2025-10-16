import gc
import warnings
from re import Pattern
from typing import Callable, List

from httpx import codes
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises, warns
from pytest_httpx import HTTPXMock

from firebolt.client import ClientV1 as Client
from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.common._types import ColType
from firebolt.db import Connection, connect
from firebolt.db.cursor import CursorV1 as Cursor
from firebolt.utils.exception import (
    AccountNotFoundError,
    ConfigurationError,
    ConnectionClosedError,
    FireboltEngineError,
    NotSupportedError,
)
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import ACCOUNT_ENGINE_ID_BY_NAME_URL


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
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
) -> None:
    """Connection initialized its cursors properly."""
    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    for url in (engine_url, f"https://{engine_url}"):
        with connect(
            engine_url=url,
            database=db_name,
            auth=UsernamePassword("u", "p"),
            api_endpoint=api_endpoint,
        ) as connection:
            cursor = connection.cursor()
            assert (
                cursor.connection == connection
            ), "Invalid cursor connection attribute"
            assert (
                cursor._client.base_url == connection._client.base_url
            ), "Invalid cursor _client attribute"

            assert cursor.execute("select*") == len(python_query_data)

            cursor.close()
            assert (
                cursor not in connection._cursors
            ), "Cursor wasn't removed from connection after close"


def test_connect_empty_parameters():
    with raises(ConfigurationError):
        with connect(engine_url="engine_url"):
            pass


def test_connect_engine_name(
    engine_name: str,
    api_endpoint: str,
    account_name: str,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    engine_id: str,
    get_engine_url_by_id_url: str,
    get_engine_url_by_id_callback: Callable,
    python_query_data: List[List[ColType]],
    account_id: str,
):
    """connect properly handles engine_name"""

    with raises(ConfigurationError):
        connect(
            engine_url="engine_url",
            engine_name="engine_name",
            database="db",
            auth=UsernamePassword("u", "p"),
        )

    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        get_engine_url_by_id_callback,
        url=get_engine_url_by_id_url,
        is_reusable=True,
    )

    # Mock engine id lookup error
    httpx_mock.add_response(
        url=f"https://{api_endpoint}"
        + ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        + f"?engine_name={engine_name}",
        status_code=codes.NOT_FOUND,
    )

    with raises(FireboltEngineError):
        connect(
            database="db",
            auth=UsernamePassword("u", "p"),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        )

    # Mock engine id lookup by name
    httpx_mock.add_response(
        url=f"https://{api_endpoint}"
        + ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        + f"?engine_name={engine_name}",
        status_code=codes.OK,
        json={"engine_id": {"engine_id": engine_id}},
    )

    with connect(
        engine_name=engine_name,
        database=db_name,
        auth=UsernamePassword("u", "p"),
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connect_default_engine(
    engine_url: str,
    api_endpoint: str,
    account_name: str,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    database_id: str,
    engine_by_db_url: str,
    python_query_data: List[List[ColType]],
    account_id: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )
    engine_by_db_url = f"{engine_by_db_url}?database_name={db_name}"

    httpx_mock.add_response(
        url=engine_by_db_url,
        status_code=codes.OK,
        json={
            "engine_url": engine_url,
        },
    )
    with connect(
        database=db_name,
        auth=UsernamePassword("u", "p"),
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connection_unclosed_warnings(client: Client):
    c = Connection("", "", client, Cursor, None, "")
    with warns(UserWarning) as winfo:
        del c
        gc.collect()

    assert any(
        "Unclosed" in str(warning.message) for warning in winfo.list
    ), "Invalid unclosed connection warning"


def test_connection_no_warnings(client: Client):
    c = Connection("", "", client, Cursor, None, "")
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
    engine_url: str,
    api_endpoint: str,
    account_name: str,
    user: str,
    password: str,
    db_name: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
    access_token: str,
    account_id_callback: Callable,
    account_id_url: str,
) -> None:
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    with Patcher():
        with connect(
            database=db_name,
            auth=UsernamePassword(user, password, use_token_cache=True),
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=user, password=password)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        with connect(
            database=db_name,
            auth=UsernamePassword(user, password, use_token_cache=False),
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(username=user, password=password)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"


def test_connect_with_auth(
    httpx_mock: HTTPXMock,
    user: str,
    password: str,
    account_name: str,
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    check_credentials_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    access_token: str,
    account_id_callback: Callable,
    account_id_url: str,
) -> None:
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    for auth in (
        UsernamePassword(
            user,
            password,
            use_token_cache=False,
        ),
        Token(access_token),
    ):
        with connect(
            auth=auth,
            database=db_name,
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("select*")


def test_connect_account_name(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    auth_url: str,
    check_credentials_callback: Callable,
    account_id_url: Pattern,
    account_id_callback: Callable,
):
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    with raises(AccountNotFoundError):
        with connect(
            auth=auth,
            database=db_name,
            engine_url=engine_url,
            account_name="invalid",
            api_endpoint=api_endpoint,
        ):
            pass

    with connect(
        auth=auth,
        database=db_name,
        engine_url=engine_url,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        pass
