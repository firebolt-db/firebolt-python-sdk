from typing import Callable, List

from httpx import codes
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises, warns
from pytest_httpx import HTTPXMock

from firebolt.async_db._types import ColType
from firebolt.client.auth import Token, UsernamePassword
from firebolt.common.settings import Settings
from firebolt.db import Connection, connect
from firebolt.utils.exception import ConfigurationError, ConnectionClosedError
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import ACCOUNT_ENGINE_BY_NAME_URL


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
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
) -> None:
    """Connection initialised its cursors properly."""
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    for url in (settings.server, f"https://{settings.server}"):
        with connect(
            engine_url=url,
            database=db_name,
            username="u",
            password="p",
            api_endpoint=settings.server,
        ) as connection:

            cursor = connection.cursor()
            assert (
                cursor.connection == connection
            ), "Invalid cursor connection attribute"
            assert (
                cursor._client == connection._client
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


def test_connect_access_token(
    settings: Settings,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    check_token_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
    access_token: str,
):
    httpx_mock.add_callback(check_token_callback, url=query_url)
    with (
        connect(
            engine_url=settings.server,
            database=db_name,
            access_token=access_token,
            account_name="a",
            api_endpoint=settings.server,
        )
    ) as connection:
        cursor = connection.cursor()
        assert cursor.execute("select*") == -1

    with raises(ConfigurationError):
        with connect(engine_url="engine_url", database="database"):
            pass

    with raises(ConfigurationError):
        with connect(
            engine_url="engine_url",
            database="database",
            username="username",
            password="password",
            access_token="access_token",
        ):
            pass


def test_connect_engine_name(
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

    with raises(ConfigurationError):
        connect(
            engine_url="engine_url",
            engine_name="engine_name",
            database="db",
            username="username",
            password="password",
        )

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(get_engine_callback, url=get_engine_url)

    engine_name = settings.server.split(".")[0]

    # Mock engine id lookup by name
    httpx_mock.add_response(
        url=f"https://{settings.server}"
        + ACCOUNT_ENGINE_BY_NAME_URL.format(account_id=account_id)
        + f"?engine_name={engine_name}",
        status_code=codes.OK,
        json={"engine_id": {"engine_id": engine_id}},
    )

    with connect(
        engine_name=engine_name,
        database=db_name,
        username="u",
        password="p",
        account_name=settings.account_name,
        api_endpoint=settings.server,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connect_default_engine(
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
    database_by_name_url: str,
    database_by_name_callback: Callable,
    database_id: str,
    engine_by_db_url: str,
    python_query_data: List[List[ColType]],
    account_id: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    engine_by_db_url = f"{engine_by_db_url}?database_name={db_name}"

    httpx_mock.add_response(
        url=engine_by_db_url,
        status_code=codes.OK,
        json={
            "engine_url": settings.server,
        },
    )
    with connect(
        database=db_name,
        username="u",
        password="p",
        account_name=settings.account_name,
        api_endpoint=settings.server,
    ) as connection:
        assert connection.cursor().execute("select*") == len(python_query_data)


def test_connection_unclosed_warnings():
    c = Connection("", "", None, "")
    with warns(UserWarning) as winfo:
        del c

    assert "Unclosed" in str(
        winfo.list[0].message
    ), "Invalid unclosed connection warning"


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
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
    access_token: str,
) -> None:
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    with Patcher():
        with connect(
            database=db_name,
            username=settings.user,
            password=settings.password.get_secret_value(),
            engine_url=settings.server,
            account_name=settings.account_name,
            api_endpoint=settings.server,
            use_token_cache=True,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(
            username=settings.user, password=settings.password.get_secret_value()
        )
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        with connect(
            database=db_name,
            username=settings.user,
            password=settings.password.get_secret_value(),
            engine_url=settings.server,
            account_name=settings.account_name,
            api_endpoint=settings.server,
            use_token_cache=False,
        ) as connection:
            assert connection.cursor().execute("select*") == len(python_query_data)
        ts = TokenSecureStorage(
            username=settings.user, password=settings.password.get_secret_value()
        )
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"


def test_connect_with_auth(
    httpx_mock: HTTPXMock,
    settings: Settings,
    db_name: str,
    check_credentials_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    access_token: str,
) -> None:
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(query_callback, url=query_url)

    for auth in (
        UsernamePassword(
            settings.user,
            settings.password.get_secret_value(),
            use_token_cache=False,
        ),
        Token(access_token),
    ):
        with connect(
            auth=auth,
            database=db_name,
            engine_url=settings.server,
            account_name=settings.account_name,
            api_endpoint=settings.server,
        ) as connection:
            connection.cursor().execute("select*")
