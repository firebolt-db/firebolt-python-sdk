from asyncio import gather
from re import Pattern, compile
from types import MethodType
from typing import Any, Callable

from httpx import Response, codes
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.client import DEFAULT_API_URL, AsyncClient
from firebolt.client.auth import Token, UsernamePassword
from firebolt.common import Settings
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import fix_url_schema


async def test_client_retry(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password)
    ) as client:

        # auth get token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": test_token},
        )

        # client request failed authorization
        httpx_mock.add_response(
            status_code=codes.UNAUTHORIZED,
        )

        # auth get another token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": test_token},
        )

        # client request success this time
        httpx_mock.add_response(
            status_code=codes.OK,
        )

        assert (
            await client.get("https://url")
        ).status_code == codes.OK, "request failed with firebolt client"


async def test_client_different_auths(
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    check_token_callback: Callable,
    test_username: str,
    test_password: str,
    test_token: str,
):
    """
    Client properly handles such auth types:
    - tuple(username, password)
    - Auth
    - None
    All other types should raise TypeError.
    """

    httpx_mock.add_callback(
        check_credentials_callback,
        url=f"https://{DEFAULT_API_URL}{AUTH_URL}",
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password)
    ) as client:
        await client.get("https://url")
    async with AsyncClient(auth=Token(test_token)) as client:
        await client.get("https://url")

    # client accepts None auth, but authorization fails
    with raises(AssertionError) as excinfo:
        async with AsyncClient(auth=None) as client:
            await client.get("https://url")

    with raises(TypeError) as excinfo:
        async with AsyncClient(auth=lambda r: r):
            await client.get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


async def test_client_account_id(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    account_id: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    auth_url: str,
    auth_callback: Callable,
    settings: Settings,
):
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password),
        base_url=fix_url_schema(settings.server),
        api_endpoint=settings.server,
    ) as c:
        assert await c.account_id == account_id, "Invalid account id returned."


@mark.asyncio
async def test_concurent_auth_lock(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    auth_url: str,
    check_credentials_callback: Callable,
    check_token_callback: Callable,
    settings: Settings,
) -> None:
    CONCURENT_COUNT = 10
    url = "https://url"

    call_count = 0

    async def mock_send_handling_redirects(self, *args: Any, **kwargs: Any) -> Response:
        nonlocal call_count
        call_count += 1
        return await AsyncClient._send_handling_redirects(self, *args, **kwargs)

    httpx_mock.add_callback(check_token_callback, url=compile(f"{url}/."))
    httpx_mock.add_callback(check_credentials_callback, url=auth_url)

    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password),
        api_endpoint=settings.server,
    ) as c:
        c._send_handling_redirects = MethodType(mock_send_handling_redirects, c)
        await gather(*[c.get(f"{url}/{i}") for i in range(CONCURENT_COUNT)])

    # 1 authorization request + CONCURENT_COUNT of GET requestsx
    assert call_count == 1 + CONCURENT_COUNT
