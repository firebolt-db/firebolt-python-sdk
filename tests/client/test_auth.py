import typing

import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from firebolt.client import Auth
from firebolt.common.exception import AuthenticationError


def test_auth_basic(
    httpx_mock: HTTPXMock,
    mocker: MockerFixture,
    check_credentials_callback: typing.Callable,
    test_username,
    test_password,
    test_token,
):
    """FireboltAuth can retrieve token and expiration values."""

    httpx_mock.add_callback(check_credentials_callback)

    mocker.patch("time.time", return_value=0)
    auth = Auth(test_username, test_password)
    assert auth.token == test_token, "invalid access token"
    assert auth._expires == 2 ** 32, "invalid expiration value"


def test_auth_refresh_on_expiration(
    httpx_mock: HTTPXMock,
    test_token: str,
    test_token2: str,
):
    """FireboltAuth refreshes the token on expiration."""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 0, "access_token": test_token2},
    )

    auth = Auth("user", "password")
    assert auth.token == test_token, "invalid access token"
    assert auth.token == test_token2, "expired access token was not updated"


def test_auth_uses_same_token_if_valid(
    httpx_mock: HTTPXMock,
    test_token: str,
    test_token2: str,
):
    """FireboltAuth refreshes the token on expiration"""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 2 ** 32, "access_token": test_token},
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 2 ** 32, "access_token": test_token2},
    )

    auth = Auth("user", "password")
    assert auth.token == test_token, "invalid access token"
    assert auth.token == test_token, "shoud not update token until it expires"
    httpx_mock.reset(False)


def test_auth_error_handling(httpx_mock: HTTPXMock):
    """FireboltAuth handles various errors properly."""

    for api_endpoint in ("https://host", "host"):
        auth = Auth("user", "password", api_endpoint=api_endpoint)

        # Internal httpx error
        def http_error(**kwargs):
            raise httpx.StreamError("httpx")

        httpx_mock.add_callback(http_error)
        with pytest.raises(AuthenticationError) as excinfo:
            auth.token

            assert (
                str(excinfo.value)
                == "Failed to authenticate at https://host: StreamError('httpx')"
            ), "Invalid authentication error message"
            httpx_mock.reset(True)

            # HTTP error
            httpx_mock.add_response(status_code=httpx.codes.BAD_REQUEST)
            with pytest.raises(AuthenticationError) as excinfo:
                auth.token

            errmsg = str(excinfo.value)
            assert (
                errmsg.startswith("Failed to authenticate at https://host:")
                and "Bad Request" in errmsg
            ), "Invalid authentication error message"
            httpx_mock.reset(True)

            # Firebolt api error
            httpx_mock.add_response(
                status_code=httpx.codes.OK, json={"error": "", "message": "firebolt"}
            )
            with pytest.raises(AuthenticationError) as excinfo:
                auth.token

            assert (
                str(excinfo.value) == "Failed to authenticate at https://host: firebolt"
            ), "Invalid authentication error message"
            httpx_mock.reset(True)


def test_auth_adds_header(
    httpx_mock: HTTPXMock,
    test_token: str,
):
    """FireboltAuth adds required authentication headers to httpx.Request."""
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    auth = Auth("user", "password")
    request = next(auth.auth_flow(httpx.Request("get", "")))
    assert "authorization" in request.headers, "missing authorization header"
    assert (
        request.headers["authorization"] == f"Bearer {test_token}"
    ), "missing authorization header"
