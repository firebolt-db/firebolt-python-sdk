from typing import Optional

from httpx import Request

from firebolt.client.auth.request_based import _RequestBased
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_URL
from firebolt.utils.util import cached_property


class ServiceAccount(_RequestBased):
    """Service account authentication class for Firebolt database.

    Gets authentication token using provided service account credentials
    (client_id/client_secret) and updates it when it expires

    Args:
        client_id (str): Service account client ID
        password (str): Service account client secret
        use_token_cache (bool): True if token should be cached in filesystem,
            False otherwise

    Attributes:
        client_id (str): Service account client ID
        password (str): Service account client secret
    """

    __slots__ = (
        "client_id",
        "client_secret",
        "_token",
        "_expires",
        "_use_token_cache",
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
            UsernamePassword: Auth object
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
            json={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
