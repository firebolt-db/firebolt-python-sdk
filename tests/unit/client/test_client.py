from typing import Callable

from httpx import Request, Timeout, codes
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client import ClientV2 as Client
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.client.resource_manager_hooks import raise_on_4xx_5xx
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from tests.unit.conftest import Response
from tests.unit.test_cache_helpers import cache_token


def test_client_retry(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    access_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    with Client(account_name=account_name, auth=auth) as client:
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
            client.get("https://url").status_code == codes.OK
        ), "request failed with firebolt client"


def test_client_different_auths(
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

    Client(account_name=account_name, auth=auth, api_endpoint=api_endpoint).get(
        "https://url"
    )

    with raises(TypeError) as excinfo:
        Client(account_name=account_name, auth=lambda r: r).get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


# FIR-14945
def test_refresh_with_hooks(
    fs: FakeFilesystem,
    httpx_mock: HTTPXMock,
    account_name: str,
    client_id: str,
    client_secret: str,
    access_token: str,
    enable_cache: Callable,
) -> None:
    """
    When hooks are used, the invalid token, fetched from cache, is refreshed
    """

    cache_token(client_id, client_secret, access_token, 2**32, account_name)

    auth = ClientCredentials(client_id, client_secret)
    auth.account = account_name

    client = Client(
        account_name=account_name,
        auth=auth,
        event_hooks={
            "response": [raise_on_4xx_5xx],
        },
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
        client.get("https://url").status_code == codes.OK
    ), "request failed with firebolt client"


def test_client_clone(
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

    with Client(
        auth=ClientCredentials(client_id, client_secret, use_token_cache=False),
        account_name=account_name,
        base_url=url,
        api_endpoint=api_endpoint,
        timeout=timeout,
        headers=headers,
    ) as c:
        c.get(path)

        # clone the client and make sure the clone works
        c2 = c.clone()
        c2.get(path)

        # not sure how to test the timeout, but at least make sure it's the same
        assert c2._timeout == timeout
