import logging

from pydantic import BaseSettings, Field, SecretStr, root_validator

from firebolt.client.auth import Auth

logger = logging.getLogger(__name__)

AUTH_CREDENTIALS_DEPRECATION_MESSAGE = """ Passing connection credentials directly in Settings is deprecated.
 Please consider passing Auth object instead.
 Examples:
  >>> from firebolt.client.auth import UsernamePassword
  >>> ...
  >>> settings = Settings(auth=UsernamePassword(username, password), ...)
 or
  >>> from firebolt.client.auth import Token
  >>> ...
  >>> settings = Settings(auth=Token(access_token), ...)"""


class Settings(BaseSettings):
    """Settings for Firebolt SDK.

    Attributes:
        user (Optional[str]): User name
        password (Optional[str]): User password
        access_token (Optional[str]): Access token to use for authentication.
            Mutually exclusive with user and password
        account_name (Optional[str]): Account name.
            Default user account is used if none provided
        server (Optional[str]): Environment api endpoint (Advanced).
            Default api endpoint is used if none provided
        default_region (str): Default region for provisioning
    """

    auth: Auth = Field(None)
    # Authorization
    user: str = Field(None, env="FIREBOLT_USER")
    password: SecretStr = Field(None, env="FIREBOLT_PASSWORD")
    # Or
    access_token: str = Field(None, env="FIREBOLT_AUTH_TOKEN")

    account_name: str = Field(None, env="FIREBOLT_ACCOUNT")
    server: str = Field(..., env="FIREBOLT_SERVER")
    default_region: str = Field(..., env="FIREBOLT_DEFAULT_REGION")
    use_token_cache: bool = Field(True)

    class Config:
        """Internal pydantic config."""

        env_file = ".env"

    @root_validator
    def mutual_exclusive_with_creds(cls, values: dict) -> dict:
        """Validate that either creds or token is provided.

        Args:
            values (dict): settings initial values

        Returns:
            dict: Validated settings values

        Raises:
            ValueError: Either both or none of credentials and token are provided
        """
        if values["user"] or values["password"]:
            if values["access_token"]:
                raise ValueError("Provide only one of user/password or access_token")
        elif not values["access_token"]:
            raise ValueError("Provide either user/password or access_token")
        return values

    @root_validator
    def deprecate_credentials(cls, values: dict) -> dict:
        """Raise deprecation warning if credentials are provided.

        Args:
            values (dict): settings initial values

        Returns:
            dict: Validated settings values

        Raises:
            ValueError: Auth is provided along with credentials
        """
        if any(
            f in values for f in ("user", "password", "access_token", "use_token_cache")
        ):
            if values.get("auth"):
                raise ValueError("Auth object is provided along with credentials")
            else:
                logger.warning(AUTH_CREDENTIALS_DEPRECATION_MESSAGE)

        return values
