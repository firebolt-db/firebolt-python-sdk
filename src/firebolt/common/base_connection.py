from __future__ import annotations

from typing import Any, Dict, List, Optional

from firebolt.async_db.cursor import BaseCursor, Cursor
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.utils.exception import ConfigurationError, ConnectionClosedError

KEEPALIVE_FLAG: int = 1

KEEPIDLE_RATE: int = 60  # seconds
DEFAULT_TIMEOUT_SECONDS: int = 60


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


def _validate_engine_name_and_url(
    engine_name: Optional[str], engine_url: Optional[str]
) -> None:
    if engine_name and engine_url:
        raise ConfigurationError(
            "Both engine_name and engine_url are provided. Provide only one to connect."
        )


class BaseConnection:
    client_class: type
    cursor_class: type
    __slots__ = (
        "_client",
        "_cursors",
        "database",
        "engine_url",
        "api_endpoint",
        "_is_closed",
    )

    def __init__(
        self,
        engine_url: str,
        database: str,
        auth: Auth,
        api_endpoint: str = DEFAULT_API_URL,
        additional_parameters: Dict[str, Any] = {},
    ):
        self.api_endpoint = api_endpoint
        self.engine_url = engine_url
        self.database = database
        self._cursors: List[BaseCursor] = []
        self._is_closed = False

    def _cursor(self, **kwargs: Any) -> BaseCursor:
        """
        Create new cursor object.
        """

        if self.closed:
            raise ConnectionClosedError("Unable to create cursor: connection closed.")

        c = self.cursor_class(self._client, self, **kwargs)
        self._cursors.append(c)
        return c

    @property
    def closed(self) -> bool:
        """`True` if connection is closed; `False` otherwise."""
        return self._is_closed

    def _remove_cursor(self, cursor: Cursor) -> None:
        # This way it's atomic
        try:
            self._cursors.remove(cursor)
        except ValueError:
            pass

    def commit(self) -> None:
        """Does nothing since Firebolt doesn't have transactions."""

        if self.closed:
            raise ConnectionClosedError("Unable to commit: Connection closed.")
