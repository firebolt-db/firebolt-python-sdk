from typing import Optional

from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.utils.exception import ConfigurationError


def _get_auth(
    username: Optional[str],
    password: Optional[str],
    access_token: Optional[str],
    use_token_cache: bool,
) -> Auth:
    """Create `Auth` class based on provided credentials.

    If `access_token` is provided, it's used for `Auth` creation.
    Otherwise, username/password are used.

    Returns:
        Auth: `auth object`

    Raises:
        `ConfigurationError`: Invalid combination of credentials provided

    """
    if not access_token:
        if not username or not password:
            raise ConfigurationError(
                "Neither username/password nor access_token are provided. Provide one"
                " to authenticate."
            )
        return UsernamePassword(username, password, use_token_cache)
    if username or password:
        raise ConfigurationError(
            "Username/password and access_token are both provided. Provide only one"
            " to authenticate."
        )
    return Token(access_token)
