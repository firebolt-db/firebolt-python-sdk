from typing import Optional

from firebolt.client.auth.base import AuthRequest
from firebolt.client.auth.request_auth_base import _RequestBasedAuth
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from firebolt.utils.util import cached_property


class ServiceAccount(_RequestBasedAuth):
    """Service Account authentication class for Firebolt Database.

    Gets authentication token using
    provided credentials and updates it when it expires.

    Args:
        client_id (str): Client ID
        client_secret (str): Client secret
        use_token_cache (bool): True if token should be cached in filesystem;
            False otherwise

    Attributes:
        client_id (str): Client ID
        client_secret (str): Client secret
    """

    __slots__ = (
        "client_id",
        "client_secret",
        "_token",
        "_expires",
        "_use_token_cache",
        "_user_agent",
    )

    requires_response_body = True

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        use_token_cache: bool = True,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        super().__init__(use_token_cache)

    def copy(self) -> "ServiceAccount":
        """Make another auth object with same credentials.

        Returns:
            ServiceAccount: Auth object
        """
        return ServiceAccount(self.client_id, self.client_secret, self._use_token_cache)

    @cached_property
    def _token_storage(self) -> Optional[TokenSecureStorage]:
        """Token filesystem cache storage.

        This is evaluated lazily, only if caching is enabled

        Returns:
            TokenSecureStorage: Token filesystem cache storage
        """
        return TokenSecureStorage(username=self.client_id, password=self.client_secret)

    def _make_auth_request(self) -> AuthRequest:
        """Get new token using username and password.

        Yields:
            Request: An http request to get token. Expects Response to be sent back

        Raises:
            AuthenticationError: Error while authenticating with provided credentials
        """

        response = self.request_class(
            "POST",
            AUTH_SERVICE_ACCOUNT_URL,
            headers={
                "User-Agent": self._user_agent,
            },
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
        return response
