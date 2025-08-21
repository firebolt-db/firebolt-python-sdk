from firebolt.client.auth.base import AuthRequest, FireboltAuthVersion
from firebolt.client.auth.request_auth_base import _RequestBasedAuth
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL


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

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        use_token_cache: bool = True,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        super().__init__(use_token_cache)

    @property
    def principal(self) -> str:
        """Get the principal (client id) associated with the auth.

        Returns:
            str: client id
        """
        return self.client_id

    @property
    def secret(self) -> str:
        """Get the secret (client secret) associated with the auth.

        Returns:
            str: client secret
        """
        return self.client_secret

    def get_firebolt_version(self) -> FireboltAuthVersion:
        """Get Firebolt version from auth.

        Returns:
            FireboltAuthVersion: V1 for Service Account authentication
        """
        return FireboltAuthVersion.V1

    def copy(self) -> "ServiceAccount":
        """Make another auth object with same credentials.

        Returns:
            ServiceAccount: Auth object
        """
        return ServiceAccount(self.client_id, self.client_secret, self._use_token_cache)

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
