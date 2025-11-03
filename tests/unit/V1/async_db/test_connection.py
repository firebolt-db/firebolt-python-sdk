from asyncio import run
from re import Pattern
from typing import Callable, List
from unittest.mock import ANY as AnyValue
from unittest.mock import patch

from httpx import codes
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.async_db.connection import Connection, connect
from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.common._types import ColType
from firebolt.utils.exception import (
    AccountNotFoundError,
    ConfigurationError,
    ConnectionClosedError,
    FireboltEngineError,
)
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import ACCOUNT_ENGINE_ID_BY_NAME_URL


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
    c1, c2 = connection.cursor(), connection.cursor()
    assert (
        len(connection._cursors) == 2
    ), "Invalid number of cursors stored in connection."

    await connection.aclose()
    assert connection.closed, "Connection was not closed on close."
    assert c1.closed, "Cursor was not closed on connection close."
    assert c2.closed, "Cursor was not closed on connection close."
    assert len(connection._cursors) == 0, "Cursors left in connection after close."
    await connection.aclose()


async def test_cursor_initialized(
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
        async with (
            await connect(
                engine_url=url,
                database=db_name,
                auth=UsernamePassword(
                    "u",
                    "p",
                ),
                api_endpoint=api_endpoint,
            )
        ) as connection:
            cursor = connection.cursor()
            assert (
                cursor.connection == connection
            ), "Invalid cursor connection attribute."
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
        async with await connect(engine_url="engine_url"):
            pass


async def test_connect_access_token(
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    check_token_callback: Callable,
    query_url: str,
    python_query_data: List[List[ColType]],
    access_token: str,
):
    httpx_mock.add_callback(
        check_token_callback,
        url=query_url,
        is_reusable=True,
    )
    async with (
        await connect(
            engine_url=engine_url,
            database=db_name,
            auth=Token(access_token),
            api_endpoint=api_endpoint,
        )
    ) as connection:
        cursor = connection.cursor()
        assert await cursor.execute("select*") == -1

    with raises(ConfigurationError):
        async with await connect(engine_url="engine_url", database="database"):
            pass


async def test_connect_engine_name(
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
        async with await connect(
            engine_url="engine_url",
            engine_name="engine_name",
            database="db",
            auth=UsernamePassword(
                "username",
                "password",
            ),
            account_name="account_name",
        ):
            pass

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
        async with await connect(
            database="db",
            auth=UsernamePassword(
                "username",
                "password",
            ),
            engine_name=engine_name,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ):
            pass

    # Mock engine id lookup by name
    httpx_mock.add_response(
        url=f"https://{api_endpoint}"
        + ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        + f"?engine_name={engine_name}",
        status_code=codes.OK,
        json={"engine_id": {"engine_id": engine_id}},
    )

    async with await connect(
        engine_name=engine_name,
        database=db_name,
        auth=UsernamePassword(
            "u",
            "p",
        ),
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert await connection.cursor().execute("select*") == len(python_query_data)


async def test_connect_default_engine(
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
    engine_by_db_url: str,
    python_query_data: List[List[ColType]],
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
    async with await connect(
        database=db_name,
        auth=UsernamePassword(
            "u",
            "p",
        ),
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        assert await connection.cursor().execute("select*") == len(python_query_data)


async def test_connection_commit(connection: Connection):
    # nothing happens
    await connection.commit()

    await connection.aclose()
    with raises(ConnectionClosedError):
        await connection.commit()


@mark.nofakefs
async def test_connection_token_caching(
    engine_url: str,
    api_endpoint: str,
    user: str,
    password: str,
    account_name: str,
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
        async with await connect(
            database=db_name,
            auth=UsernamePassword(
                user,
                password,
                use_token_cache=True,
            ),
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert await connection.cursor().execute("select*") == len(
                python_query_data
            )
        ts = TokenSecureStorage(username=user, password=password)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        async with await connect(
            database=db_name,
            auth=UsernamePassword(
                user,
                password,
                use_token_cache=False,
            ),
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            assert await connection.cursor().execute("select*") == len(
                python_query_data
            )
        ts = TokenSecureStorage(username=user, password=password)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"


async def test_connect_with_auth(
    httpx_mock: HTTPXMock,
    engine_url: str,
    api_endpoint: str,
    user: str,
    account_name: str,
    password: str,
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
        async with await connect(
            auth=auth,
            database=db_name,
            engine_url=engine_url,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")


async def test_connect_account_name(
    httpx_mock: HTTPXMock,
    username_password_auth: Auth,
    engine_url: str,
    api_endpoint: str,
    account_name: str,
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
        async with await connect(
            auth=username_password_auth,
            database=db_name,
            engine_url=engine_url,
            account_name="invalid",
            api_endpoint=api_endpoint,
        ):
            pass

    async with await connect(
        auth=username_password_auth,
        database=db_name,
        engine_url=engine_url,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        pass


async def test_connect_with_user_agent(
    httpx_mock: HTTPXMock,
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    query_callback: Callable,
    query_url: str,
    access_token: str,
) -> None:
    with patch("firebolt.common.base_connection.get_user_agent_header") as ut:
        ut.return_value = "MyConnector/1.0 DriverA/1.1"
        httpx_mock.add_callback(
            query_callback,
            url=query_url,
            match_headers={"User-Agent": "MyConnector/1.0 DriverA/1.1"},
        )

        async with await connect(
            auth=Token(access_token),
            database=db_name,
            engine_url=engine_url,
            api_endpoint=api_endpoint,
            additional_parameters={
                "user_clients": [("MyConnector", "1.0")],
                "user_drivers": [("DriverA", "1.1")],
            },
        ) as connection:
            await connection.cursor().execute("select*")
        ut.assert_called_once_with(
            [("DriverA", "1.1")], [("MyConnector", "1.0")], AnyValue
        )


async def test_connect_no_user_agent(
    httpx_mock: HTTPXMock,
    engine_url: str,
    api_endpoint: str,
    db_name: str,
    query_callback: Callable,
    query_url: str,
    access_token: str,
) -> None:
    with patch("firebolt.common.base_connection.get_user_agent_header") as ut:
        ut.return_value = "Python/3.0"
        httpx_mock.add_callback(
            query_callback, url=query_url, match_headers={"User-Agent": "Python/3.0"}
        )

        async with await connect(
            auth=Token(access_token),
            database=db_name,
            engine_url=engine_url,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("select*")
        ut.assert_called_once_with([], [], AnyValue)


def test_from_asyncio(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    query_callback: Callable,
    query_url: str,
    engine_url: str,
    api_endpoint: str,
    db_name: str,
):
    async def async_flow() -> None:
        async with (
            await connect(
                engine_url=engine_url,
                database=db_name,
                auth=UsernamePassword(
                    "u",
                    "p",
                ),
                api_endpoint=api_endpoint,
            )
        ) as connection:
            cursor = connection.cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
            await cursor.fetchmany(1)
            await cursor.fetchall()

    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )
    run(async_flow())
