from typing import Optional

from httpx import Request

from firebolt.client.auth.request_based import _RequestBased
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import cached_property


class UsernamePassword(_RequestBased):
    """Username/Password authentication class for Firebolt database.

    Gets authentication token using
    provided credentials and updates it when it expires

    Args:
        username (str): Username
        password (str): Password
        use_token_cache (bool): True if token should be cached in filesystem,
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

    def _make_auth_request(self) -> Request:
        return self.request_class(
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
