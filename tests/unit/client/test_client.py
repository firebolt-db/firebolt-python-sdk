from typing import Callable

from httpx import codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client import DEFAULT_API_URL, Auth, Client
from firebolt.common import Settings
from firebolt.common.urls import AUTH_URL
from firebolt.common.util import fix_url_schema


def test_client_retry(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with Unauthorized error
    """
    client = Client(auth=(test_username, test_password))

    # auth get token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2 ** 30, "access_token": test_token},
    )

    # client request failed authorization
    httpx_mock.add_response(
        status_code=codes.UNAUTHORIZED,
    )

    # auth get another token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2 ** 30, "access_token": test_token},
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
    test_username: str,
    test_password: str,
):
    """
    Client propperly handles such auth types:
    - tuple(username, password)
    - Auth
    - None
    All other types should raise TypeError
    """

    httpx_mock.add_callback(
        check_credentials_callback,
        url=AUTH_URL.format(api_endpoint=f"https://{DEFAULT_API_URL}"),
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    Client(auth=(test_username, test_password)).get("https://url")
    Client(auth=Auth(test_username, test_password)).get("https://url")

    # client accepts None auth, but authorization fails
    with raises(AssertionError) as excinfo:
        Client(auth=None).get("https://url")

    with raises(TypeError) as excinfo:
        Client(auth=lambda r: r).get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


def test_client_account_id(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    account_id: str,
    account_id_url: str,
    account_id_callback: Callable,
    auth_url: str,
    auth_callback: Callable,
    settings: Settings,
):
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    with Client(
        auth=(test_username, test_password),
        base_url=fix_url_schema(settings.server),
        api_endpoint=settings.server,
    ) as c:
        assert c.account_id == account_id, "Invalid account id returned"
