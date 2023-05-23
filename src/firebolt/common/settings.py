import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from firebolt.client.auth import Auth, UsernamePassword

logger = logging.getLogger(__name__)

KEEPALIVE_FLAG: int = 1

KEEPIDLE_RATE: int = 60  # seconds
DEFAULT_TIMEOUT_SECONDS: int = 60

AUTH_CREDENTIALS_DEPRECATION_MESSAGE = """ Passing connection credentials directly in Settings is deprecated.
 Use Auth object instead.
 Examples:
  >>> from firebolt.client.auth import UsernamePassword
  >>> ...
  >>> settings = Settings(auth=UsernamePassword(username, password), ...)
 or
  >>> from firebolt.client.auth import Token
  >>> ...
  >>> settings = Settings(auth=Token(access_token), ...)"""

USERNAME_ENV = "FIREBOLT_USER"
PASSWORD_ENV = "FIREBOLT_PASSWORD"
AUTH_TOKEN_ENV = "FIREBOLT_AUTH_TOKEN"
ACCOUNT_ENV = "FIREBOLT_ACCOUNT"
SERVER_ENV = "FIREBOLT_SERVER"
DEFAULT_REGION_ENV = "FIREBOLT_DEFAULT_REGION"


def from_env(var_name: str, default: Any = None) -> Callable:
    def inner() -> Any:
        os.environ.get(var_name, default)

    return inner


def auth_from_env() -> Optional[Auth]:
    username = os.environ.get(USERNAME_ENV, None)
    password = os.environ.get(PASSWORD_ENV, None)
    if username and password:
        return UsernamePassword(username, password)
    return None


@dataclass
class Settings:
    """Settings for Firebolt SDK.

    Attributes:
        user (Optional[str]): User name
        password (Optional[str]): User password
        access_token (Optional[str]): Access token to use for authentication
            Mutually exclusive with user and password
        account_name (Optional[str]): Account name
            Default user account is used if none provided
        server (Optional[str]): Environment api endpoint (Advanced)
            Default api endpoint is used if none provided
        default_region (str): Default region for provisioning
    """

    auth: Optional[Auth] = field(default_factory=auth_from_env)
    # Authorization
    user: Optional[str] = field(default=None)
    password: Optional[str] = field(default=None)
    # Or
    access_token: Optional[str] = field(default_factory=from_env(AUTH_TOKEN_ENV))

    account_name: Optional[str] = field(default_factory=from_env(ACCOUNT_ENV))
    server: str = field(default_factory=from_env(SERVER_ENV))
    default_region: str = field(default_factory=from_env(DEFAULT_REGION_ENV))
    use_token_cache: bool = field(default=True)

    def __post_init__(self) -> None:
        """Validate that either creds or token is provided.

        Args:
            values (dict): settings initial values

        Returns:
            dict: Validated settings values

        Raises:
            ValueError: Either both or none of credentials and token are provided
        """

        params_present = (
            self.user is not None or self.password is not None,
            self.access_token is not None,
            self.auth is not None,
        )
        if sum(params_present) == 0:
            raise ValueError(
                "Provide at least one of auth, user/password or access_token."
            )
        if sum(params_present) > 1:
            raise ValueError("Provide only one of auth, user/password or access_token")
        if any((self.user, self.password, self.access_token)):
            logger.warning(AUTH_CREDENTIALS_DEPRECATION_MESSAGE)
