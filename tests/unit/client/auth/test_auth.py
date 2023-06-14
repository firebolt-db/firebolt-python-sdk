from types import MethodType
from unittest.mock import PropertyMock, patch

from httpx import Request, codes
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth
from firebolt.utils.token_storage import TokenSecureStorage
from tests.unit.util import execute_generator_requests


def test_auth_refresh_on_expiration(
    httpx_mock: HTTPXMock, access_token: str, access_token_2: str
) -> None:
    """Auth refreshes the token on expiration."""
    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url)

    # Mock auth flow
    def set_token(token: str) -> callable:
        def inner(self):
            self._token = token
            self._expires = 0
            yield from ()

        return inner

    auth = Auth(use_token_cache=False)
    # Get token for the first time
    auth.get_new_token_generator = MethodType(set_token(access_token), auth)
    execute_generator_requests(auth.auth_flow(Request("GET", url)))
    assert auth.token == access_token, "invalid access token"
    assert auth.expired

    # Refresh token
    auth.get_new_token_generator = MethodType(set_token(access_token_2), auth)
    execute_generator_requests(auth.auth_flow(Request("GET", url)))
    assert auth.token == access_token_2, "Expired access token was not updated."


def test_auth_uses_same_token_if_valid(
    httpx_mock: HTTPXMock, access_token: str, access_token_2: str
) -> None:
    """Auth reuses the token until it's expired."""
    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url)

    # Mock auth flow
    def set_token(token: str) -> callable:
        def inner(self):
            self._token = token
            self._expires = 2**32
            yield from ()

        return inner

    auth = Auth(use_token_cache=False)
    # Get token for the first time
    auth.get_new_token_generator = MethodType(set_token(access_token), auth)
    execute_generator_requests(auth.auth_flow(Request("GET", url)))
    assert auth.token == access_token, "invalid access token"
    assert not auth.expired

    # Refresh token
    auth.get_new_token_generator = MethodType(set_token(access_token_2), auth)
    execute_generator_requests(auth.auth_flow(Request("GET", url)))
    assert auth.token == access_token, "Should not update token until it expires."


def test_auth_adds_header(access_token: str) -> None:
    """Auth adds required authentication headers to httpx.Request."""
    auth = Auth(use_token_cache=False)
    auth._token = access_token
    auth._expires = 2**32
    flow = auth.auth_flow(Request("get", ""))
    request = next(flow)

    assert "authorization" in request.headers, "missing authorization header"
    assert (
        request.headers["authorization"] == f"Bearer {access_token}"
    ), "missing authorization header"


@mark.nofakefs
def test_auth_token_storage(
    httpx_mock: HTTPXMock,
    client_id: str,
    client_secret: str,
    access_token: str,
) -> None:
    # Mock auth flow
    def set_token(token: str) -> callable:
        def inner(self):
            self._token = token
            self._expires = 2**32
            yield from ()

        return inner

    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url)
    with Patcher(), patch(
        "firebolt.client.auth.base.Auth._token_storage",
        new_callable=PropertyMock,
        return_value=TokenSecureStorage(client_id, client_secret),
    ):
        auth = Auth(use_token_cache=True)
        # Get token
        auth.get_new_token_generator = MethodType(set_token(access_token), auth)
        execute_generator_requests(auth.auth_flow(Request("GET", url)))

        st = TokenSecureStorage(client_id, client_secret)
        assert st.get_cached_token() == access_token, "Invalid token value cached"

    with Patcher(), patch(
        "firebolt.client.auth.base.Auth._token_storage",
        new_callable=PropertyMock,
        return_value=TokenSecureStorage(client_id, client_secret),
    ):
        auth = Auth(use_token_cache=False)
        # Get token
        auth.get_new_token_generator = MethodType(set_token(access_token), auth)
        execute_generator_requests(auth.auth_flow(Request("GET", url)))
        st = TokenSecureStorage(client_id, client_secret)
        assert (
            st.get_cached_token() is None
        ), "Token cached even though caching is disabled"
