import random
from queue import Queue
from re import Pattern, compile
from types import MethodType
from typing import Any, Callable

from httpx import Request, codes
from pytest import raises
from pytest_httpx import HTTPXMock
from trio import open_nursery, sleep

from firebolt.client import DEFAULT_API_URL
from firebolt.client import AsyncClientV1 as AsyncClient
from firebolt.client.auth import Token, UsernamePassword
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import fix_url_schema
from tests.unit.conftest import Response, retry_if_failed


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
        is_reusable=True,
    )

    httpx_mock.add_callback(
        check_token_callback,
        url="https://url",
        is_reusable=True,
    )

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
    api_endpoint: str,
):
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)

    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password),
        base_url=fix_url_schema(api_endpoint),
        api_endpoint=api_endpoint,
    ) as c:
        assert await c.account_id == account_id, "Invalid account id returned."


async def test_concurent_auth_lock(
    httpx_mock: HTTPXMock,
    api_endpoint: str,
    test_username: str,
    test_password: str,
    test_token: str,
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
            json={"expires_in": 2**32, "access_token": test_token},
        )

    httpx_mock.add_callback(
        check_token_callback,
        url=compile(f"{url}/."),
        is_reusable=True,
    )
    httpx_mock.add_callback(check_credentials, url=auth_url, is_reusable=True)

    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password, False),
        api_endpoint=api_endpoint,
    ) as c:
        c._send_handling_redirects = MethodType(mock_send_handling_redirects, c)
        urls = [f"{url}/{i}" for i in range(CONCURENT_COUNT)]
        async with open_nursery() as nursery:
            for url in urls:
                nursery.start_soon(c.get, url)

    assert checked_creds_times == 1


# test that client requests are truly concurrent
# and are executed not in order that they were started
# but in order of completion
@retry_if_failed(3)
async def test_true_concurent_requests(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    auth_url: str,
    auth_callback: Callable,
    api_endpoint: str,
):
    url = "https://url"
    CONCURENT_COUNT = 10

    queue = Queue(CONCURENT_COUNT)

    # create callback that uses check_token_callback but also pushes URl to a queue
    async def check_token_callback_with_queue(request: Request, **kwargs) -> Response:
        nonlocal queue
        queue.put(str(request.url))
        return Response(status_code=codes.OK, headers={"content-length": "0"})

    async def mock_send_handling_redirects(self, *args: Any, **kwargs: Any) -> Response:
        # simulate network delay so the context switches
        # random delay to make sure that requests are not executed in order
        await sleep(random.random())
        return await AsyncClient._send_handling_redirects(self, *args, **kwargs)

    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)

    httpx_mock.add_callback(
        check_token_callback_with_queue,
        url=compile(f"{url}/."),
        is_reusable=True,
    )

    urls = [f"{url}/{i}" for i in range(CONCURENT_COUNT)]
    async with AsyncClient(
        auth=UsernamePassword(test_username, test_password),
        api_endpoint=api_endpoint,
    ) as c:
        c._send_handling_redirects = MethodType(mock_send_handling_redirects, c)
        async with open_nursery() as nursery:
            for url in urls:
                nursery.start_soon(c.get, url)

    assert queue.qsize() == CONCURENT_COUNT
    # Make sure the order is random and not sequential
    assert list(queue.queue) != urls
    # Cover the case when requests might be queued in reverse order
    urls.reverse()
    assert list(queue.queue) != urls
