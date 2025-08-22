from types import MethodType
from typing import Generator

from httpx import Request, codes
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth, ClientCredentials
from firebolt.utils.cache import _firebolt_cache
from tests.unit.test_cache_helpers import get_cached_token
from tests.unit.util import async_execute_generator_requests


async def test_auth_refresh_on_expiration(
    httpx_mock: HTTPXMock, access_token: str, access_token_2: str
) -> None:
    """Auth refreshes the token on expiration."""
    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url, is_reusable=True)

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
    await async_execute_generator_requests(auth.async_auth_flow(Request("GET", url)))
    assert auth.token == access_token, "invalid access token"
    assert auth.expired

    # Refresh token
    auth.get_new_token_generator = MethodType(set_token(access_token_2), auth)
    await async_execute_generator_requests(auth.async_auth_flow(Request("GET", url)))
    assert auth.token == access_token_2, "expired access token was not updated"


async def test_auth_uses_same_token_if_valid(
    httpx_mock: HTTPXMock, access_token: str, access_token_2: str
) -> None:
    """Auth reuses the token until it's expired."""
    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url, is_reusable=True)

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
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == access_token, "invalid access token"
    assert not auth.expired

    auth.get_new_token_generator = MethodType(set_token(access_token_2), auth)
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == access_token, "shoud not update token until it expires"


async def test_auth_adds_header(access_token: str) -> None:
    """Auth adds required authentication headers to httpx.Request."""
    auth = Auth(use_token_cache=False)
    auth._token = access_token
    auth._expires = 2**32
    flow = auth.async_auth_flow(Request("get", ""))
    request = await flow.__anext__()

    assert "authorization" in request.headers, "missing authorization header"
    assert (
        request.headers["authorization"] == f"Bearer {access_token}"
    ), "missing authorization header"


@mark.nofakefs
async def test_auth_token_storage(
    httpx_mock: HTTPXMock,
    client_id: str,
    client_secret: str,
    access_token: str,
    enable_cache: Generator,
) -> None:
    # Mock auth flow
    def set_token(token: str) -> callable:
        def inner(self):
            self._token = token
            self._expires = 2**32
            yield from ()

        return inner

    url = "https://host"
    httpx_mock.add_response(status_code=codes.OK, url=url, is_reusable=True)

    # Test with caching enabled
    with Patcher():
        auth = ClientCredentials(client_id, client_secret, use_token_cache=True)
        # Get token
        auth.get_new_token_generator = MethodType(set_token(access_token), auth)
        await async_execute_generator_requests(
            auth.async_auth_flow(Request("GET", url))
        )

        # Verify token was cached using the new cache system
        cached_token = get_cached_token(client_id, client_secret, None)
        assert cached_token == access_token, "Invalid token value cached"

    # Clear cache before second test
    _firebolt_cache.clear()

    # Test with caching disabled
    with Patcher():
        auth = ClientCredentials(client_id, client_secret, use_token_cache=False)
        # Get token
        auth.get_new_token_generator = MethodType(set_token(access_token), auth)
        await async_execute_generator_requests(
            auth.async_auth_flow(Request("GET", url))
        )

        # Verify token was not cached
        cached_token = get_cached_token(client_id, client_secret, None)
        assert cached_token is None, "Token cached even though caching is disabled"
