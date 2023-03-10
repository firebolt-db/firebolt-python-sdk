from re import Pattern
from typing import Callable

from httpx import codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client import DEFAULT_API_URL, AsyncClient
from firebolt.client.auth import Auth
from firebolt.common import Settings
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from firebolt.utils.util import fix_url_schema


async def test_client_retry(
    httpx_mock: HTTPXMock,
    auth: Auth,
    access_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    async with AsyncClient(auth=auth) as client:
        # auth get token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": access_token},
        )

        # client request failed authorization
        httpx_mock.add_response(
            status_code=codes.UNAUTHORIZED,
        )

        # auth get another token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": access_token},
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
    auth: Auth,
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
        url=f"https://{DEFAULT_API_URL}{AUTH_SERVICE_ACCOUNT_URL}",
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    async with AsyncClient(auth=auth) as client:
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
    auth: Auth,
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
        auth=auth,
        base_url=fix_url_schema(settings.server),
        api_endpoint=settings.server,
    ) as c:
        assert await c.account_id == account_id, "Invalid account id returned."
