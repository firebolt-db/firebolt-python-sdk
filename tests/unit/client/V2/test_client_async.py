from re import Pattern, compile
from types import MethodType
from typing import Any, Callable

from httpx import Request, codes
from pytest import raises
from pytest_httpx import HTTPXMock
from trio import open_nursery, sleep

from firebolt.client import AsyncClientV2 as AsyncClient
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from firebolt.utils.util import fix_url_schema
from tests.unit.conftest import Response


async def test_client_retry(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    access_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    async with AsyncClient(account_name=account_name, auth=auth) as client:
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
    account_name: str,
    auth_server: str,
    server: str,
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
        url=f"https://{auth_server}{AUTH_SERVICE_ACCOUNT_URL}",
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    async with AsyncClient(
        account_name=account_name, auth=auth, api_endpoint=server
    ) as client:
        await client.get("https://url")

    with raises(TypeError) as excinfo:
        async with AsyncClient(
            account_name=account_name, auth=lambda r: r, api_endpoint=server
        ):
            await client.get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


async def test_client_account_id(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    account_id: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    auth_url: str,
    auth_callback: Callable,
    server: str,
):
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    async with AsyncClient(
        account_name=account_name,
        auth=auth,
        base_url=fix_url_schema(server),
        api_endpoint=server,
    ) as c:
        assert await c.account_id == account_id, "Invalid account id returned."


async def test_concurent_auth_lock(
    httpx_mock: HTTPXMock,
    account_name: str,
    server: str,
    client_id: str,
    client_secret: str,
    access_token: str,
    auth_url: str,
    check_token_callback: Callable,
) -> None:
    CONCURENT_COUNT = 10
    url = "https://url"

    checked_creds_times = 0

    async def mock_send_handling_redirects(self, *args: Any, **kwargs: Any) -> Response:
        # simulate network delay so the context switches
        await sleep(0.01)
        return await AsyncClient._send_handling_redirects(self, *args, **kwargs)

    def check_credentials(
        request: Request = None,
        **kwargs,
    ) -> Response:
        nonlocal checked_creds_times
        checked_creds_times += 1
        return Response(
            status_code=codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    httpx_mock.add_callback(check_token_callback, url=compile(f"{url}/."))
    httpx_mock.add_callback(check_credentials, url=auth_url)

    async with AsyncClient(
        auth=ClientCredentials(client_id, client_secret),
        api_endpoint=server,
        account_name=account_name,
    ) as c:
        c._send_handling_redirects = MethodType(mock_send_handling_redirects, c)
        urls = [f"{url}/{i}" for i in range(CONCURENT_COUNT)]
        async with open_nursery() as nursery:
            for url in urls:
                nursery.start_soon(c.get, url)

    assert checked_creds_times == 1
