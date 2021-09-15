import json

import httpx
import pytest
from pytest_httpx import HTTPXMock, to_response

from firebolt.client import DEFAULT_API_URL, FireboltAuth, FireboltClient

TEST_TOKEN: str = "test_token"
TEST_USERNAME: str = "username"
TEST_PASSWORD: str = "password"


def test_client_retry(httpx_mock: HTTPXMock):
    """
    Firebolt client retries with new auth token
    if first attempt fails with Unauthorized error
    """
    client = FireboltClient(auth=(TEST_USERNAME, TEST_PASSWORD))

    # auth get token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expiry": 2 ** 30, "access_token": TEST_TOKEN},
    )

    # client request failed authorization
    httpx_mock.add_response(
        status_code=httpx.codes.UNAUTHORIZED,
    )

    # auth get another token
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
        json={"expiry": 2 ** 30, "access_token": TEST_TOKEN},
    )

    # client request success this time
    httpx_mock.add_response(
        status_code=httpx.codes.OK,
    )

    assert (
        client.get("https://url").status_code == httpx.codes.OK
    ), "request failed with firebolt client"


def test_client_different_auths(httpx_mock: HTTPXMock):
    """
    Firebolt propperly handles such auth types:
    - tuple(username, password)
    - FireboltAuth
    - None
    All other types should raise TypeError
    """

    def check_credentials(
        request: httpx.Request = None, **kwargs
    ) -> httpx.Response:
        assert request, "empty request"
        body = json.loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == TEST_USERNAME, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == TEST_PASSWORD, "Invalid password"

        return to_response(
            status_code=httpx.codes.OK,
            json={"expiry": 2 ** 32, "access_token": TEST_TOKEN},
        )

    def check_token(request: httpx.Request = None, **kwargs) -> httpx.Response:
        prefix = "Bearer "
        assert request, "empty request"
        assert (
            "authorization" in request.headers
        ), "missing authorization header"
        auth = request.headers["authorization"]
        assert auth.startswith(prefix), "invalid authorization header format"
        token = auth[len(prefix) :]
        assert token == TEST_TOKEN, "invalid authorization token"

        return to_response(status_code=httpx.codes.OK)

    httpx_mock.add_callback(
        check_credentials, url=f"https://{DEFAULT_API_URL}/auth/v1/login"
    )

    httpx_mock.add_callback(check_token, url="https://url")

    FireboltClient(auth=(TEST_USERNAME, TEST_PASSWORD)).get("https://url")
    FireboltClient(auth=FireboltAuth(TEST_USERNAME, TEST_PASSWORD)).get(
        "https://url"
    )

    # client accepts None auth, but authorization fails
    with pytest.raises(AssertionError) as excinfo:
        FireboltClient(auth=None).get("https://url")

    with pytest.raises(TypeError) as excinfo:
        FireboltClient(auth=lambda r: r).get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"
