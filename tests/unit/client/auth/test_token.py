from typing import Callable

from httpx import Request, Response, codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Token
from firebolt.utils.exception import AuthorizationError
from tests.unit.util import execute_generator_requests


def test_token_happy_path(
    httpx_mock: HTTPXMock, test_token: str, check_token_callback: Callable
) -> None:
    """Validate that provided token is added to request."""
    httpx_mock.add_callback(check_token_callback)

    auth = Token(test_token)
    execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))


def test_token_invalid(httpx_mock: HTTPXMock) -> None:
    """Authorization error raised when token is invalid."""

    def authorization_error(*args, **kwargs) -> Response:
        return Response(status_code=codes.UNAUTHORIZED)

    httpx_mock.add_callback(authorization_error)

    auth = Token("token")
    with raises(AuthorizationError):
        execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))


def test_token_expired() -> None:
    """Authorization error is raised when token expires."""
    auth = Token("token")
    auth._expires = 0
    with raises(AuthorizationError):
        execute_generator_requests(auth.auth_flow(Request("GET", "https://host")))
