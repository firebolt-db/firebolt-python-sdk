from time import time
from typing import Generator, Optional

from httpx import Auth as HttpxAuth
from httpx import Request, Response, codes

from firebolt.client.constants import _REQUEST_ERRORS, DEFAULT_API_URL
from firebolt.common.exception import AuthenticationError
from firebolt.common.token_storage import TokenSecureStorage
from firebolt.common.urls import AUTH_URL
from firebolt.common.util import cached_property, fix_url_schema


class Auth(HttpxAuth):
    """Authentication class for Firebolt database.

    Gets authentication token using
    provided credentials and updates it when it expires

    Args:
        username (str): Username
        password (str): Password
        api_endpoint (Optional[str]): Environment api endpoint.
            Default api.app.firebolt.io

    Attributes:
        username (str): Username
        password (str): Password
    """

    __slots__ = (
        "username",
        "password",
        "_api_endpoint",
        "_token",
        "_expires",
        "_use_token_cache",
    )

    requires_response_body = True

    @staticmethod
    def from_token(token: str) -> "Auth":
        """Create auth based on already acquired token.

        Args:
            token (str): Bearer token

        Returns:
            Auth: Auth object
        """
        a = Auth("", "", use_token_cache=False)
        a._token = token
        return a

    def __init__(
        self,
        username: str,
        password: str,
        api_endpoint: str = DEFAULT_API_URL,
        use_token_cache: bool = True,
    ):
        self.username = username
        self.password = password
        self._use_token_cache = use_token_cache

        # Add schema to url if it's missing
        self._api_endpoint = fix_url_schema(api_endpoint)
        self._token: Optional[str] = self._get_cached_token()
        self._expires: Optional[int] = None

    def copy(self) -> "Auth":
        """Make another auth object with same credentials.

        Returns:
            Auth: Auth object
        """
        return Auth(self.username, self.password, self._api_endpoint)

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
    def _token_storage(self) -> TokenSecureStorage:
        """Token filesystem cache storage.

        This is evaluated lazily, only if caching is enabled

        Returns:
            TokenSecureStorage: Token filesystem cache storage
        """
        return TokenSecureStorage(username=self.username, password=self.password)

    def _get_cached_token(self) -> Optional[str]:
        """If caching enabled, get token from filesystem cache.

        If caching is disabled, None is returned

        Returns:
            Optional[str]: Token if any and if caching is enabled, None otherwise
        """
        if not self._use_token_cache:
            return None
        return self._token_storage.get_cached_token()

    def _cache_token(self) -> None:
        """If caching enabled, cache token to filesystem."""
        if not self._use_token_cache:
            return
        # Only cache if token and expiration are retrieved
        if self._token and self._expires:
            self._token_storage.cache_token(self._token, self._expires)

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Get new token using username and password.

        Yields:
            Request: An http request to get token. Expects Response to be sent back

        Raises:
            AuthenticationError: Error while authenticating with provided credentials
        """
        try:
            response = yield Request(
                "POST",
                AUTH_URL.format(api_endpoint=self._api_endpoint),
                headers={
                    "Content-Type": "application/json;charset=UTF-8",
                    "User-Agent": "firebolt-sdk",
                },
                json={"username": self.username, "password": self.password},
            )
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time()) + int(parsed["expires_in"])
            self._cache_token()

        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e), self._api_endpoint) from e

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

        request.headers["Authorization"] = f"Bearer {self.token}"

        response = yield request

        if response.status_code == codes.UNAUTHORIZED:
            yield from self.get_new_token_generator()
            request.headers["Authorization"] = f"Bearer {self.token}"
            yield request

    def _check_response_error(self, response: dict) -> None:
        """Check if response data contains errors.

        Args:
            response (dict): Response data

        Raises:
            AuthenticationError: Were unable to authenticate
        """
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
                self._api_endpoint,
            )
