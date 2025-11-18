import random
from queue import Queue
from re import compile
from types import MethodType
from typing import Any, Callable

from httpx import Request, Timeout, codes
from pytest import raises
from pytest_httpx import HTTPXMock
from trio import open_nursery, sleep

from firebolt.client import AsyncClientV2 as AsyncClient
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from tests.unit.conftest import Response, retry_if_failed


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
    auth_endpoint,
    api_endpoint: str,
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
        url=f"https://{auth_endpoint}{AUTH_SERVICE_ACCOUNT_URL}",
        is_reusable=True,
    )

    httpx_mock.add_callback(
        check_token_callback,
        url="https://url",
        is_reusable=True,
    )

    async with AsyncClient(
        account_name=account_name, auth=auth, api_endpoint=api_endpoint
    ) as client:
        await client.get("https://url")

    with raises(TypeError) as excinfo:
        async with AsyncClient(
            account_name=account_name, auth=lambda r: r, api_endpoint=api_endpoint
        ):
            await client.get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


async def test_concurent_auth_lock(
    httpx_mock: HTTPXMock,
    account_name: str,
    api_endpoint: str,
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

    httpx_mock.add_callback(
        check_token_callback,
        url=compile(f"{url}/."),
        is_reusable=True,
    )
    httpx_mock.add_callback(check_credentials, url=auth_url, is_reusable=True)

    async with AsyncClient(
        auth=ClientCredentials(client_id, client_secret),
        api_endpoint=api_endpoint,
        account_name=account_name,
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
    account_name: str,
    client_id: str,
    client_secret: str,
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
        auth=ClientCredentials(client_id, client_secret),
        api_endpoint=api_endpoint,
        account_name=account_name,
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


async def test_client_clone(
    httpx_mock: HTTPXMock,
    client_id: str,
    client_secret: str,
    account_name: str,
    api_endpoint: str,
    access_token: str,
    auth_url: str,
    check_credentials_callback: Callable,
    check_token_callback: Callable,
):
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)

    url = "https://base_url"
    path = "/path"
    headers = {"User-Agent": "test"}
    timeout = Timeout(123, read=None)

    def validate_client_callback(request: Request, **kwargs) -> Response:
        check_token_callback(request)
        assert [request.headers[k] == v for k, v in headers.items()]
        return Response(status_code=codes.OK, headers={"content-length": "0"})

    httpx_mock.add_callback(
        validate_client_callback,
        url=url + path,
        is_reusable=True,
    )

    async with AsyncClient(
        auth=ClientCredentials(client_id, client_secret, use_token_cache=False),
        account_name=account_name,
        base_url=url,
        api_endpoint=api_endpoint,
        timeout=timeout,
        headers=headers,
    ) as c:
        await c.get(path)

        # clone the client and make sure the clone works
        c2 = c.clone()
        await c2.get(path)

        # not sure how to test the timeout, but at least make sure it's the same
        assert c2._timeout == timeout
