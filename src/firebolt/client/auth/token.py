from typing import Generator

from httpx import Request, Response

from firebolt.client.auth import Auth
from firebolt.utils.exception import AuthorizationError


class Token(Auth):
    """Token authentication class for Firebolt Database.

    Uses provided token for authentication. Doesn't cache token and doesn't
    refresh it on expiration.

    Args:
        token (str): Authorization token

    Attributes:
        token (str):
    """

    def __init__(self, token: str):
        super().__init__(use_token_cache=False)
        self._token = token

    def copy(self) -> "Token":
        """Make another auth object with same credentials.

        Returns:
            Token: Auth object
        """
        assert self.token
        return Token(self.token)

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Raise authorization error since token is invalid or expired.

        Raises:
            AuthorizationError: Token is invalid or expired
        """
        raise AuthorizationError("Provided token in not valid anymore.")
