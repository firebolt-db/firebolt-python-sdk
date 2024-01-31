import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from firebolt.client.auth import Auth, ClientCredentials

logger = logging.getLogger(__name__)

CLIENT_ID_ENV = "FIREBOLT_CLIENT_ID"
CLIENT_SECRET_ENV = "FIREBOLT_CLIENT_SECRET"
ACCOUNT_ENV = "FIREBOLT_ACCOUNT"
SERVER_ENV = "FIREBOLT_SERVER"
DEFAULT_REGION_ENV = "FIREBOLT_DEFAULT_REGION"


def from_env(var_name: str, default: Any = None) -> Callable:
    def inner() -> Any:
        return os.environ.get(var_name, default)

    return inner


def auth_from_env() -> Auth:
    client_id = os.environ.get(CLIENT_ID_ENV, None)
    client_secret = os.environ.get(CLIENT_SECRET_ENV, None)
    if client_id and client_secret:
        return ClientCredentials(client_id, client_secret)
    raise ValueError("Auth not provided")


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

    auth: Auth = field(default_factory=auth_from_env)

    account_name: str = field(default_factory=from_env(ACCOUNT_ENV))
    server: str = field(default_factory=from_env(SERVER_ENV))
    default_region: str = field(default_factory=from_env(DEFAULT_REGION_ENV))
