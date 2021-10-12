import json
import typing

import httpx
import pytest
from pytest_httpx import to_response
from pytest_httpx._httpx_internals import Response


@pytest.fixture
def test_token() -> str:
    yield "test_token"


@pytest.fixture
def test_token2() -> str:
    yield "test_token2"


@pytest.fixture
def test_username() -> str:
    yield "username"


@pytest.fixture
def test_password() -> str:
    yield "password"


@pytest.fixture
def check_credentials_callback(
    test_username: str, test_password: str, test_token: str
) -> typing.Callable:
    def check_credentials(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = json.loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == test_username, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == test_password, "Invalid password"

        return to_response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2 ** 32, "access_token": test_token},
        )

    return check_credentials


@pytest.fixture
def check_token_callback(test_token: str) -> typing.Callable:
    def check_token(request: httpx.Request = None, **kwargs) -> Response:
        prefix = "Bearer "
        assert request, "empty request"
        assert "authorization" in request.headers, "missing authorization header"
        auth = request.headers["authorization"]
        assert auth.startswith(prefix), "invalid authorization header format"
        token = auth[len(prefix) :]
        assert token == test_token, "invalid authorization token"

        return to_response(status_code=httpx.codes.OK)

    return check_token
