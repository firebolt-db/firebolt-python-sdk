from typing import Optional

from firebolt.client.auth.base import AuthRequest
from firebolt.client.auth.request_auth_base import _RequestBasedAuth
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import cached_property


class UsernamePassword(_RequestBasedAuth):
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
        "_user_agent",
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

    def _make_auth_request(self) -> AuthRequest:
        """Get new token using username and password.

        Yields:
            Request: An http request to get token. Expects Response to be sent back

        Raises:
            AuthenticationError: Error while authenticating with provided credentials
        """
        response = self.request_class(
            "POST",
            AUTH_URL,
            headers={
                "Content-Type": "application/json;charset=UTF-8",
                "User-Agent": self._user_agent,
            },
            json={"username": self.username, "password": self.password},
        )
        return response
