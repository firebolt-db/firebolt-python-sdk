from asyncio import Lock as ALock
from time import time
from typing import AsyncGenerator, Generator, Optional

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
        use_token_cache (bool): True if token should be cached in filesystem,
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
        self._async_auth_lock = ALock()

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

        This is evaluated lazily, only if caching is enabled

        Returns:
            Optional[TokenSecureStorage]: Token filesystem cache storage if any
        """
        return None

    def _get_cached_token(self) -> Optional[str]:
        """If caching enabled, get token from filesystem cache.

        If caching is disabled, None is returned

        Returns:
            Optional[str]: Token if any and if caching is enabled, None otherwise
        """
        if not self._use_token_cache or not self._token_storage:
            return None
        return self._token_storage.get_cached_token()

    def _cache_token(self) -> None:
        """If caching enabled, cache token to filesystem."""
        if not self._use_token_cache or not self._token_storage:
            return
        # Only cache if token and expiration are retrieved
        if self._token and self._expires:
            self._token_storage.cache_token(self._token, self._expires)

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Generate requests needed to create a new token session."""
        raise NotImplementedError()

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

    def sync_authentication(
        self, force: bool = False
    ) -> Generator[Request, Response, None]:
        if not self.token or self.expired or force:
            flow = self.get_new_token_generator()
            response = None
            while True:
                try:
                    request = (
                        flow.send(response) if response else next(flow)  # type: ignore
                    )
                except StopIteration:
                    break
                response = yield request
                response.read()
            self._cache_token()

    def sync_auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Execute the authentication flow synchronously.

        Sycnronously fetches the new token in these cases:
         - We haven't yet fetched id
         - It's expired
         - In case a request, authorized with previous token,
           failed with 401 Unauthorized error. Only one attempt to reauthenticate
           is performed, if the error persists, it will just be raised

        Args:
            request (Request): A request that should be authorized

        Yields:
            Request: Request, that should be executed for auth flow
        """
        yield from self.sync_authentication()
        request.headers["Authorization"] = f"Bearer {self.token}"
        response = yield request

        if response.status_code == codes.UNAUTHORIZED:
            yield from self.sync_authentication(force=True)
            request.headers["Authorization"] = f"Bearer {self.token}"
            yield request

    async def async_auth_flow(
        self, request: Request
    ) -> AsyncGenerator[Request, Response]:
        """Execute the authentication flow asynchronously.

        Asycnronously fetches the new token in these cases:
         - We haven't yet fetched id
         - It's expired
         - In case a request, authorized with previous token,
           failed with 401 Unauthorized error. Only one attempt to reauthenticate
           is performed, if the error persists, it will just be raised

        Args:
            request (Request): A request that should be authorized

        Yields:
            Request: Request, that should be executed for auth flow
        """
        response: Optional[Response] = None
        attempts = 0
        while (
            response is None or response.status_code == codes.UNAUTHORIZED
        ) and attempts < 2:
            request_failed = response and response.status_code == codes.UNAUTHORIZED
            async with self._async_auth_lock:
                if not self.token or self.expired or request_failed:
                    flow = self.get_new_token_generator()
                    auth_response: Optional[Response] = None
                    while True:
                        try:
                            auth_request = (
                                flow.send(auth_response)
                                if auth_response
                                else next(flow)
                            )
                        except StopIteration:
                            break
                        auth_response = yield auth_request
                        await auth_response.aread()
                    self._cache_token()

            request.headers["Authorization"] = f"Bearer {self.token}"
            response = yield request
            attempts += 1
