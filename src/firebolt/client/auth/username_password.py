from time import time
from typing import Generator, Optional

from httpx import Request, Response

from firebolt.client.auth.base import Auth
from firebolt.client.constants import _REQUEST_ERRORS
from firebolt.utils.exception import AuthenticationError
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import cached_property


class UsernamePassword(Auth):
    """Username/Password authentication class for Firebolt Database.

    Gets authentication token using
    provided credentials and updates it when it expires.

    Args:
        username (str): Username
        password (str): Password
        use_token_cache (bool): True if token should be cached in filesystem;
            False otherwise

    Attributes:
        username (str): Username
        password (str): Password
    """

    __slots__ = (
        "username",
        "password",
        "_token",
        "_expires",
        "_use_token_cache",
    )

    requires_response_body = True

    def __init__(
        self,
        username: str,
        password: str,
        use_token_cache: bool = True,
    ):
        self.username = username
        self.password = password
        super().__init__(use_token_cache)

    def copy(self) -> "UsernamePassword":
        """Make another auth object with same credentials.

        Returns:
            UsernamePassword: Auth object
        """
        return UsernamePassword(self.username, self.password, self._use_token_cache)

    @cached_property
    def _token_storage(self) -> Optional[TokenSecureStorage]:
        """Token filesystem cache storage.

        This is evaluated lazily, only if caching is enabled

        Returns:
            TokenSecureStorage: Token filesystem cache storage
        """
        return TokenSecureStorage(username=self.username, password=self.password)

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Get new token using username and password.

        Yields:
            Request: An http request to get token. Expects Response to be sent back

        Raises:
            AuthenticationError: Error while authenticating with provided credentials
        """
        try:
            response = yield self.request_class(
                "POST",
                # The full url is generated on client side by attaching
                # it to api_endpoint
                AUTH_URL,
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

        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e)) from e

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
            )
