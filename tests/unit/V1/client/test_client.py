from re import Pattern
from typing import Callable

from httpx import codes
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client import DEFAULT_API_URL
from firebolt.client import ClientV1 as Client
from firebolt.client.auth import Token, UsernamePassword
from firebolt.client.resource_manager_hooks import raise_on_4xx_5xx
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import fix_url_schema
from tests.unit.test_cache_helpers import cache_token


def test_client_retry(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    client = Client(auth=UsernamePassword(test_username, test_password))

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
        client.get("https://url").status_code == codes.OK
    ), "request failed with firebolt client"


def test_client_different_auths(
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

    Client(auth=UsernamePassword(test_username, test_password)).get("https://url")
    Client(auth=Token(test_token)).get("https://url")

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

    with Client(
        auth=UsernamePassword(test_username, test_password),
        base_url=fix_url_schema(api_endpoint),
        api_endpoint=api_endpoint,
    ) as c:
        assert c.account_id == account_id, "Invalid account id returned"


# FIR-14945
def test_refresh_with_hooks(
    fs: FakeFilesystem,
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token: str,
    enable_cache: Callable,
) -> None:
    """
    When hooks are used, the invalid token, fetched from cache, is refreshed
    """

    cache_token(test_username, test_password, test_token, 2**32)
    auth = UsernamePassword(test_username, test_password)
    # Simulate what connect() would do
    auth.account = None

    client = Client(
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
        json={"expires_in": 2**30, "access_token": test_token},
    )

    # client request success this time
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    assert (
        client.get("https://url").status_code == codes.OK
    ), "request failed with firebolt client"
