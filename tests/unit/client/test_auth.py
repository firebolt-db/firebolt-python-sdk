import typing

import pytest
from httpx import Client, Request, StreamError, codes
from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from firebolt.client import Auth
from firebolt.common.exception import AuthenticationError
from firebolt.common.token_storage import TokenSecureStorage
from tests.unit.util import execute_generator_requests


def test_auth_basic(
    httpx_mock: HTTPXMock,
    mocker: MockerFixture,
    check_credentials_callback: typing.Callable,
    test_username,
    test_password,
    test_token,
):
    """Auth can retrieve token and expiration values."""

    httpx_mock.add_callback(check_credentials_callback)

    mocker.patch("firebolt.client.auth.auth.time", return_value=0)
    auth = Auth(test_username, test_password)
    execute_generator_requests(auth.get_new_token_generator())
    assert auth.token == test_token, "invalid access token"
    assert auth._expires == 2**32, "invalid expiration value"


def test_auth_refresh_on_expiration(
    httpx_mock: HTTPXMock, test_token: str, test_token2: str, fs: FakeFilesystem
):
    """Auth refreshes the token on expiration."""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token2},
    )

    auth = Auth("user", "password")
    execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
    assert auth.token == test_token, "invalid access token"
    execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
    assert auth.token == test_token2, "Expired access token was not updated."


def test_auth_uses_same_token_if_valid(
    httpx_mock: HTTPXMock, test_token: str, test_token2: str, fs: FakeFilesystem
):
    """Auth refreshes the token on expiration"""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2**32, "access_token": test_token},
    )

    # Request
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2**32, "access_token": test_token2},
    )

    # Request
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    auth = Auth("user", "password")
    execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
    assert auth.token == test_token, "invalid access token"
    execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
    assert auth.token == test_token, "Should not update token until it expires."
    httpx_mock.reset(False)


def test_auth_error_handling(httpx_mock: HTTPXMock, fs: FakeFilesystem):
    """Auth handles various errors properly."""

    for api_endpoint in ("https://host", "host"):
        auth = Auth("user", "password", api_endpoint=api_endpoint)

        # Internal httpx error
        def http_error(*args, **kwargs):
            raise StreamError("httpx")

        httpx_mock.add_callback(http_error)
        with pytest.raises(StreamError) as excinfo:
            execute_generator_requests(auth.get_new_token_generator())

        assert str(excinfo.value) == "httpx", "Invalid authentication error message"
        httpx_mock.reset(True)

        # HTTP error
        httpx_mock.add_response(status_code=codes.BAD_REQUEST)
        with pytest.raises(AuthenticationError) as excinfo:
            execute_generator_requests(auth.get_new_token_generator())

        errmsg = str(excinfo.value)
        assert "Bad Request" in errmsg, "Invalid authentication error message"
        httpx_mock.reset(True)

        # Firebolt api error
        httpx_mock.add_response(
            status_code=codes.OK, json={"error": "", "message": "firebolt"}
        )
        with pytest.raises(AuthenticationError) as excinfo:
            execute_generator_requests(auth.get_new_token_generator())

        httpx_mock.reset(True)


def test_auth_adds_header(
    httpx_mock: HTTPXMock,
    test_token: str,
):
    """Auth adds required authentication headers to httpx.Request."""
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    auth = Auth("user", "password")
    with Client() as client:
        flow = auth.auth_flow(Request("get", ""))
        request = next(flow)
        response = client.send(request)
        request = flow.send(response)

    assert "authorization" in request.headers, "missing authorization header"
    assert (
        request.headers["authorization"] == f"Bearer {test_token}"
    ), "missing authorization header"


def test_auth_token_storage(
    httpx_mock: HTTPXMock,
    test_username: str,
    test_password: str,
    test_token,
) -> None:
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2 ^ 32, "access_token": test_token},
    )
    with Patcher():
        auth = Auth(test_username, test_password, use_token_cache=True)
        execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))

        st = TokenSecureStorage(test_username, test_password)
        assert st.get_cached_token() == test_token, "Invalid token value cached"

    with Patcher():
        auth = Auth(test_username, test_password, use_token_cache=False)
        execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
        st = TokenSecureStorage(test_username, test_password)
        assert (
            st.get_cached_token() is None
        ), "Token cached even though caching is disabled"
