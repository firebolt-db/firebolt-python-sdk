import typing

import httpx
import pytest
from pytest_httpx import HTTPXMock

from firebolt.client import DEFAULT_API_URL, Auth, Client


def test_client_retry(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token: str,
):
    """
    Firebolt client retries with new auth token
    if first attempt fails with Unauthorized error
    """
    client = Client(auth=(test_username, test_password))

    # auth get token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 2 ** 30, "access_token": test_token},
    )

    # client request failed authorization
    httpx_mock.add_response(
        status_code=httpx.codes.UNAUTHORIZED,
    )

    # auth get another token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 2 ** 30, "access_token": test_token},
    )

    # client request success this time
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
    )

    assert (
        client.get("https://url").status_code == httpx.codes.OK
    ), "request failed with firebolt client"


def test_client_different_auths(
    httpx_mock: HTTPXMock,
    check_credentials_callback: typing.Callable,
    check_token_callback: typing.Callable,
    test_username: str,
    test_password: str,
):
    """
    Firebolt propperly handles such auth types:
    - tuple(username, password)
    - FireboltAuth
    - None
    All other types should raise TypeError
    """

    httpx_mock.add_callback(
        check_credentials_callback,
        url=f"https://{DEFAULT_API_URL}/auth/v1/login",
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    Client(auth=(test_username, test_password)).get("https://url")
    Client(auth=Auth(test_username, test_password)).get("https://url")

    # client accepts None auth, but authorization fails
    with pytest.raises(AssertionError) as excinfo:
        Client(auth=None).get("https://url")

    with pytest.raises(TypeError) as excinfo:
        Client(auth=lambda r: r).get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"
