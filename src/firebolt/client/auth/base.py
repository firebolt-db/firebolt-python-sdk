import logging
from abc import abstractmethod
from enum import IntEnum
from time import time
from typing import AsyncGenerator, Generator, Optional

from anyio import Lock
from httpx import Auth as HttpxAuth
from httpx import Request, Response, codes

from firebolt.utils.cache import (
    ConnectionInfo,
    SecureCacheKey,
    _firebolt_cache,
)
from firebolt.utils.util import Timer, get_internal_error_code

logger = logging.getLogger(__name__)


class FireboltAuthVersion(IntEnum):
    """Enum for Firebolt authentication versions."""

    V1 = 1  # Service Account, Username Password
    V2 = 2  # Client Credentials
    CORE = 3  # Firebolt Core


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
        "_account_name",
        "_expires",
        "_use_token_cache",
    )

    requires_response_body = True
    request_class = AuthRequest

    def __init__(self, use_token_cache: bool = True):
        self._use_token_cache = use_token_cache
        self._account_name: Optional[str] = None
        self._token: Optional[str] = None
        self._expires: Optional[int] = None
        self._lock = Lock()

    def copy(self) -> "Auth":
        """Make another auth object with same credentials.

        Returns:
            Auth: Auth object
        """
        return self.__class__(self._use_token_cache)

    @property
    def token(self) -> Optional[str]:
        """Acquired bearer token.

        Returns:
            Optional[str]: Acquired token
        """
        return self._token

    @property
    @abstractmethod
    def principal(self) -> str:
        """Get the principal (username or id) associated with the token.

        Returns:
            str: Principal string
        """

    @property
    @abstractmethod
    def secret(self) -> str:
        """Get the secret (password or secret key) associated with the token.

        Returns:
            str: Secret string
        """

    @abstractmethod
    def get_firebolt_version(self) -> FireboltAuthVersion:
        """Get Firebolt version from auth.

        Returns:
            FireboltAuthVersion: The authentication version enum
        """

    @property
    def expired(self) -> bool:
        """Check if current token is expired.

        Returns:
            bool: True if expired, False otherwise
        """
        return self._expires is not None and self._expires <= int(time())

    def _get_cached_token(self) -> Optional[str]:
        """If caching is enabled, get token from cache.

        If caching is disabled, None is returned.

        Returns:
            Optional[str]: Token if any, and if caching is enabled; None otherwise
        """
        if not self._use_token_cache:
            return None

        cache_key = SecureCacheKey(
            [self.principal, self.secret, self._account_name], self.secret
        )
        connection_info = _firebolt_cache.get(cache_key)

        if connection_info and connection_info.token:
            return connection_info.token

        return None

    def _cache_token(self) -> None:
        """If caching is enabled, cache token."""
        if not self._use_token_cache:
            return
        # Only cache if token is retrieved
        if self._token:
            cache_key = SecureCacheKey(
                [self.principal, self.secret, self._account_name], self.secret
            )

            # Get existing connection info or create new one
            connection_info = _firebolt_cache.get(cache_key)
            if connection_info is None:
                connection_info = ConnectionInfo(
                    id="NONE"
                )  # This is triggered first so there will be no id

            # Update token information
            connection_info.token = self._token

            # Cache it
            _firebolt_cache.set(cache_key, connection_info)

    @abstractmethod
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
        with Timer("[PERFORMANCE] Authentication "):
            if not self.token or self.expired:
                yield from self.get_new_token_generator()
                self._cache_token()

            request.headers["Authorization"] = f"Bearer {self.token}"

            response = yield request

            if (
                response.status_code == codes.UNAUTHORIZED
                or get_internal_error_code(response) == codes.UNAUTHORIZED
            ):
                yield from self.get_new_token_generator()
                request.headers["Authorization"] = f"Bearer {self.token}"
                yield request

    async def async_auth_flow(
        self, request: Request
    ) -> AsyncGenerator[Request, Response]:
        """
        Execute the authentication flow asynchronously.

        Overridden in order to lock and ensure no more than
        one authentication request is sent at a time. This
        avoids excessive load on the auth server.
        It also makes sure to read the response body in case of an error status code
        """
        if self.requires_request_body:
            await request.aread()

        if not self.token or self.expired:
            await self._lock.acquire()
            # If another task has already updated the token,
            # we don't need to hold the lock
            if self.token and not self.expired:
                self._lock.release()

        flow = self.auth_flow(request)
        request = next(flow)

        while True:
            response = yield request
            if self.requires_response_body or codes.is_error(response.status_code):
                await response.aread()

            try:
                request = flow.send(response)
            except StopIteration:
                break
            finally:
                # token gets updated only after flow.send is called
                # so unlock only after that
                self._release_lock()

    def _release_lock(self) -> None:
        """Release the lock if held."""
        if self._lock.locked():
            try:
                self._lock.release()
            except RuntimeError as e:
                # Check the error string since RuntimeError is very generic
                if "a Lock you don't own" not in str(e):
                    raise
                # This task does not own the lock, can't release
                logging.warning("Tried to release a lock not owned by the current task")

    def sync_auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Execute the authentication flow synchronously.

        Overridden in order to ensure reading the response body
        in case of an error status code
        """
        if self.requires_request_body:
            request.read()

        flow = self.auth_flow(request)
        request = next(flow)

        while True:
            response = yield request
            if self.requires_response_body or codes.is_error(response.status_code):
                response.read()

            try:
                request = flow.send(response)
            except StopIteration:
                break
