from __future__ import annotations

import logging
import socket
from json import JSONDecodeError
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type

from httpcore.backends.auto import AutoBackend
from httpcore.backends.base import AsyncNetworkStream
from httpx import AsyncHTTPTransport, HTTPStatusError, RequestError, Timeout

from firebolt.async_db.cursor import BaseCursor, Cursor
from firebolt.client import DEFAULT_API_URL, AsyncClient
from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.utils.exception import (
    ConfigurationError,
    ConnectionClosedError,
    FireboltEngineError,
    InterfaceError,
)
from firebolt.utils.urls import (
    ACCOUNT_ENGINE_ID_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_ENGINE_URL_BY_DATABASE_NAME,
)
from firebolt.utils.usage_tracker import get_user_agent_header
from firebolt.utils.util import fix_url_schema

DEFAULT_TIMEOUT_SECONDS: int = 60
KEEPALIVE_FLAG: int = 1
KEEPIDLE_RATE: int = 60  # seconds
AUTH_CREDENTIALS_DEPRECATION_MESSAGE = """ Passing connection credentials
 directly to the `connect` function is deprecated.
 Pass the `Auth` object instead.
 Examples:
  >>> from firebolt.client.auth import UsernamePassword
  >>> ...
  >>> connect(auth=UsernamePassword(username, password), ...)
 or
  >>> from firebolt.client.auth import Token
  >>> ...
  >>> connect(auth=Token(access_token), ...)"""

logger = logging.getLogger(__name__)


async def _resolve_engine_url(
    engine_name: str,
    auth: Auth,
    api_endpoint: str,
    account_name: Optional[str] = None,
) -> str:
    async with AsyncClient(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        account_id = await client.account_id
        url = ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        try:
            response = await client.get(
                url=url,
                params={"engine_name": engine_name},
            )
            response.raise_for_status()
            engine_id = response.json()["engine_id"]["engine_id"]
            url = ACCOUNT_ENGINE_URL.format(account_id=account_id, engine_id=engine_id)
            response = await client.get(url=url)
            response.raise_for_status()
            return response.json()["engine"]["endpoint"]
        except HTTPStatusError as e:
            # Engine error would be 404.
            if e.response.status_code != 404:
                raise InterfaceError(
                    f"Error {e.__class__.__name__}: Unable to retrieve engine "
                    f"endpoint {url}."
                )
            # Once this is point is reached we've already authenticated with
            # the backend so it's safe to assume the cause of the error is
            # missing engine.
            raise FireboltEngineError(f"Firebolt engine {engine_name} does not exist.")
        except (JSONDecodeError, RequestError, RuntimeError, HTTPStatusError) as e:
            raise InterfaceError(
                f"Error {e.__class__.__name__}: "
                f"Unable to retrieve engine endpoint {url}."
            )


async def _get_database_default_engine_url(
    database: str,
    auth: Auth,
    api_endpoint: str,
    account_name: Optional[str] = None,
) -> str:
    async with AsyncClient(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        try:
            account_id = await client.account_id
            response = await client.get(
                url=ACCOUNT_ENGINE_URL_BY_DATABASE_NAME.format(account_id=account_id),
                params={"database_name": database},
            )
            response.raise_for_status()
            return response.json()["engine_url"]
        except (
            JSONDecodeError,
            RequestError,
            RuntimeError,
            HTTPStatusError,
            KeyError,
        ) as e:
            raise InterfaceError(f"Unable to retrieve default engine endpoint: {e}.")


def _validate_engine_name_and_url(
    engine_name: Optional[str], engine_url: Optional[str]
) -> None:
    if engine_name and engine_url:
        raise ConfigurationError(
            "Both engine_name and engine_url are provided. Provide only one to connect."
        )


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


def async_connect_factory(connection_class: Type) -> Callable:
    async def connect_inner(
        database: str = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        auth: Auth = None,
        engine_name: Optional[str] = None,
        engine_url: Optional[str] = None,
        account_name: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
        use_token_cache: bool = True,
        additional_parameters: Dict[str, Any] = {},
    ) -> Connection:
        """Connect to Firebolt database.

        Args:
            `database` (str): Name of the database to connect
            `username` (Optional[str]): User name to use for authentication (Deprecated)
            `password` (Optional[str]): Password to use for authentication (Deprecated)
            `access_token` (Optional[str]): Authentication token to use instead of
                                          credentials (Deprecated)
            `auth` (Auth)L Authentication object.
            `engine_name` (Optional[str]): Name of the engine to connect to
            `engine_url` (Optional[str]): The engine endpoint to use
            `account_name` (Optional[str]): For customers with multiple accounts;
                                          if none, default is used
            `api_endpoint` (str): Firebolt API endpoint. Used for authentication
            `use_token_cache` (bool): Cached authentication token in filesystem
                                    Default: True
            `additional_parameters` (Optional[Dict]): Dictionary of less widely-used
                                    arguments for connection

        Note:
            Providing both `engine_name` and `engine_url` will result in an error

        """
        # These parameters are optional in function signature
        # but are required to connect.
        # PEP 249 recommends making them kwargs.
        if not database:
            raise ConfigurationError("database name is required to connect.")

        _validate_engine_name_and_url(engine_name, engine_url)

        if not auth:
            if any([username, password, access_token, api_endpoint, use_token_cache]):
                logger.warning(AUTH_CREDENTIALS_DEPRECATION_MESSAGE)
                auth = _get_auth(username, password, access_token, use_token_cache)
            else:
                raise ConfigurationError("No authentication provided.")
        api_endpoint = fix_url_schema(api_endpoint)

        # Mypy checks, this should never happen
        assert database is not None

        if not engine_name and not engine_url:
            engine_url = await _get_database_default_engine_url(
                database=database,
                auth=auth,
                account_name=account_name,
                api_endpoint=api_endpoint,
            )

        elif engine_name:
            engine_url = await _resolve_engine_url(
                engine_name=engine_name,
                auth=auth,
                account_name=account_name,
                api_endpoint=api_endpoint,
            )
        elif account_name:
            # In above if branches account name is validated since it's used to
            # resolve or get an engine url.
            # We need to manually validate account_name if none of the above
            # cases are triggered.
            async with AsyncClient(
                auth=auth,
                base_url=api_endpoint,
                account_name=account_name,
                api_endpoint=api_endpoint,
            ) as client:
                await client.account_id

        assert engine_url is not None

        engine_url = fix_url_schema(engine_url)
        return connection_class(
            engine_url, database, auth, api_endpoint, additional_parameters
        )

    return connect_inner


class OverriddenHttpBackend(AutoBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
    ) -> AsyncNetworkStream:
        stream = await super().connect_tcp(
            host, port, timeout=timeout, local_address=local_address
        )
        # Enable keepalive
        stream.get_extra_info("socket").setsockopt(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, KEEPALIVE_FLAG
        )
        # MacOS does not have TCP_KEEPIDLE
        if hasattr(socket, "TCP_KEEPIDLE"):
            keepidle = socket.TCP_KEEPIDLE
        else:
            keepidle = 0x10  # TCP_KEEPALIVE on mac

        # Set keepalive to 60 seconds
        stream.get_extra_info("socket").setsockopt(
            socket.IPPROTO_TCP, keepidle, KEEPIDLE_RATE
        )
        return stream


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
        # Override tcp keepalive settings for connection
        transport = AsyncHTTPTransport()
        transport._pool._network_backend = OverriddenHttpBackend()
        user_drivers = additional_parameters.get("user_drivers", [])
        user_clients = additional_parameters.get("user_clients", [])
        self._client = AsyncClient(
            auth=auth,
            base_url=engine_url,
            api_endpoint=api_endpoint,
            timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
            transport=transport,
            headers={"User-Agent": get_user_agent_header(user_drivers, user_clients)},
        )
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

    async def _aclose(self) -> None:
        """Close connection and all underlying cursors."""
        if self.closed:
            return

        # self._cursors is going to be changed during closing cursors
        # after this point no cursors would be added to _cursors, only removed since
        # closing lock is held, and later connection will be marked as closed
        cursors = self._cursors[:]
        for c in cursors:
            # Here c can already be closed by another thread,
            # but it shouldn't raise an error in this case
            c.close()
        await self._client.aclose()
        self._is_closed = True

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


class Connection(BaseConnection):
    """
    Firebolt asynchronous database connection class. Implements `PEP 249`_.

    Args:
        `engine_url`: Firebolt database engine REST API url
        `database`: Firebolt database name
        `username`: Firebolt account username
        `password`: Firebolt account password
        `api_endpoint`: Optional. Firebolt API endpoint used for authentication
        `connector_versions`: Optional. Tuple of connector name and version, or
            a list of tuples of your connector stack. Useful for tracking custom
            connector usage.

    Note:
        Firebolt does not support transactions,
        so commit and rollback methods are not implemented.

    .. _PEP 249:
        https://www.python.org/dev/peps/pep-0249/

    """

    cursor_class = Cursor

    aclose = BaseConnection._aclose

    def cursor(self) -> Cursor:
        c = super()._cursor()
        assert isinstance(c, Cursor)  # typecheck
        return c

    # Context manager support
    async def __aenter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed.")
        return self

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        await self._aclose()


connect = async_connect_factory(Connection)
