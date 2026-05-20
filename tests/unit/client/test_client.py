import socket
import time

from typing import Callable

from httpx import Request, Timeout, codes
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest import raises, fixture
from pytest_httpx import HTTPXMock

from firebolt.client import ClientV2 as Client
from firebolt.client.auth import Auth, ClientCredentials, FireboltCore
from firebolt.client.http_backend import KeepaliveTransport
from firebolt.client.resource_manager_hooks import raise_on_4xx_5xx
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from tests.unit.conftest import Response


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
) -> None:
    """
    When hooks are used, the invalid token, fetched from cache, is refreshed
    """

    tss = TokenSecureStorage(client_id, client_secret)
    tss.cache_token(access_token, 2**32)

    client = Client(
        account_name=account_name,
        auth=ClientCredentials(client_id, client_secret),
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


@fixture(autouse=True)
def clear_dns_cache():
    # Always clear cache between test runs to avoid unwanted side effects
    KeepaliveTransport._dns_cache.cache.clear()
    KeepaliveTransport._dns_cache.expiry.clear()
    KeepaliveTransport._dns_cache.indices.clear()
    yield


@fixture
def mock_dns(monkeypatch):
    def mock_gethost(*args):
        return ("my-db-service", [], ["10.0.0.1", "10.0.0.2"])
    monkeypatch.setattr(socket, "gethostbyname_ex", mock_gethost)


def test_client_side_lb_round_robin(mock_dns, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url="http://10.0.0.1/query", status_code=200, text="pod-1", is_reusable=True)
    httpx_mock.add_response(url="http://10.0.0.2/query", status_code=200, text="pod-2")

    client = Client(auth=FireboltCore(), account_name="", client_side_lb=True)

    # 1. Request -> should go to 10.0.0.1 (sorted IPs)
    r1 = client.get("http://my-db-service/query")
    assert r1.text == "pod-1"

    # 2. Request -> should go to 10.0.0.2
    r2 = client.get("http://my-db-service/query")
    assert r2.text == "pod-2"

    # 3. Request -> should go to 10.0.0.1
    r3 = client.get("http://my-db-service/query")
    assert r3.text == "pod-1"


def test_dns_stale_cache_on_failure(monkeypatch, httpx_mock: HTTPXMock):
    ips = ["10.0.0.1"]

    def mock_gethost_success(*args):
        return ("service", [], ips)

    def mock_gethost_fail(*args):
        raise socket.gaierror("DNS Timeout")

    monkeypatch.setattr(socket, "gethostbyname_ex", mock_gethost_success)
    httpx_mock.add_response(url="http://10.0.0.1/query", is_reusable=True)

    client = Client(auth=FireboltCore(), account_name="", client_side_lb=True)
    client.get("http://my-db-service/query")

    monkeypatch.setattr(socket, "gethostbyname_ex", mock_gethost_fail)

    # On DNS timeout, we re-use the stale IP from the cache (best effort)
    response = client.get("http://my-db-service/query")
    assert response.status_code == 200


def test_lb_disabled_behavior(mock_dns, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url="http://my-db-service/query", text="standard")

    client = Client(auth=FireboltCore(), account_name="", client_side_lb=False)
    r = client.get("http://my-db-service/query")

    assert r.text == "standard"
    # Ensure that no IP based routing happened
    assert len(httpx_mock.get_requests(url="http://10.0.0.1/query")) == 0
