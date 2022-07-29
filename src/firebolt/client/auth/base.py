from time import time
from typing import Generator, Optional

from httpx import Auth as HttpxAuth
from httpx import Request, Response, codes

from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.util import cached_property


class AuthRequest(Request):
    """Class to distinguish auth requests from regular"""


class Auth(HttpxAuth):
    """Base authentication class for Firebolt database.

    Updates all http requests with bearer token authorization header

    Args:
        use_token_cache (bool): True if token should be cached in filesystem;
            False otherwise
    """

    __slots__ = (
        "_token",
        "_expires",
        "_use_token_cache",
    )

    requires_response_body = True
    request_class = AuthRequest

    def __init__(self, use_token_cache: bool = True):
        self._use_token_cache = use_token_cache
        self._token: Optional[str] = self._get_cached_token()
        self._expires: Optional[int] = None

    def copy(self) -> "Auth":
        """Make another auth object with same credentials.

        Returns:
            Auth: Auth object
        """
        return Auth(self._use_token_cache)

    @property
    def token(self) -> Optional[str]:
        """Acquired bearer token.

        Returns:
            Optional[str]: Acquired token
        """
        return self._token

    @property
    def expired(self) -> bool:
        """Check if current token is expired.

        Returns:
            bool: True if expired, False otherwise
        """
        return self._expires is not None and self._expires <= int(time())

    @cached_property
    def _token_storage(self) -> Optional[TokenSecureStorage]:
        """Token filesystem cache storage.

        This is evaluated lazily, only if caching is enabled.

        Returns:
            Optional[TokenSecureStorage]: Token filesystem cache storage if any
        """
        return None

    def _get_cached_token(self) -> Optional[str]:
        """If caching is enabled, get token from filesystem cache.

        If caching is disabled, None is returned.

        Returns:
            Optional[str]: Token if any, and if caching is enabled; None otherwise
        """
        if not self._use_token_cache or not self._token_storage:
            return None
        return self._token_storage.get_cached_token()

    def _cache_token(self) -> None:
        """If caching isenabled, cache token to filesystem."""
        if not self._use_token_cache or not self._token_storage:
            return
        # Only cache if token and expiration are retrieved
        if self._token and self._expires:
            self._token_storage.cache_token(self._token, self._expires)

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Generate requests needed to create a new token session."""

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Add authorization token to request headers.

        Overrides ``httpx.Auth.auth_flow``

        Args:
            request (Request): Request object to update

        Yields:
            Request: Request required for auth flow
        """
        if not self.token or self.expired:
            yield from self.get_new_token_generator()
            self._cache_token()

        request.headers["Authorization"] = f"Bearer {self.token}"

        response = yield request

        if response.status_code == codes.UNAUTHORIZED:
            yield from self.get_new_token_generator()
            request.headers["Authorization"] = f"Bearer {self.token}"
            yield request
