from __future__ import annotations

import socket
from types import TracebackType
from typing import Any, Dict, List, Optional, Type

from httpcore.backends.auto import AutoBackend
from httpcore.backends.base import AsyncNetworkStream
from httpx import AsyncHTTPTransport, Timeout

from firebolt.async_db.cursor import Cursor, CursorV1, CursorV2
from firebolt.async_db.util import _get_system_engine_url
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import Auth
from firebolt.client.client import AsyncClient, AsyncClientV1, AsyncClientV2
from firebolt.common.base_connection import BaseConnection
from firebolt.common.settings import (
    DEFAULT_TIMEOUT_SECONDS,
    KEEPALIVE_FLAG,
    KEEPIDLE_RATE,
)
from firebolt.utils.exception import (
    ConfigurationError,
    ConnectionClosedError,
    EngineNotRunningError,
    InterfaceError,
)
from firebolt.utils.usage_tracker import get_user_agent_header
from firebolt.utils.util import (
    Timer,
    fix_url_schema,
    validate_engine_name_and_url_v1,
)


class OverriddenHttpBackend(AutoBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    async def connect_tcp(  # type: ignore [override]
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        **kwargs: Any,
    ) -> AsyncNetworkStream:
        stream = await super().connect_tcp(  # type: ignore [call-arg]
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            **kwargs,
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

    client_class: type
    cursor_type: Type[Cursor]
    __slots__ = (
        "_client",
        "_cursors",
        "database",
        "engine_url",
        "api_endpoint",
        "_is_closed",
        "_system_engine_connection",
        "client_class",
        "cursor_type",
    )

    def __init__(
        self,
        engine_url: str,
        database: Optional[str],
        client: AsyncClient,
        cursor_type: Type[Cursor],
        system_engine_connection: Optional["Connection"],
        api_endpoint: str,
    ):
        super().__init__()
        self.api_endpoint = api_endpoint
        self.engine_url = engine_url
        self.database = database
        self.cursor_type = cursor_type
        self._cursors: List[Cursor] = []
        self._system_engine_connection = system_engine_connection
        # Override tcp keepalive settings for connection
        transport = AsyncHTTPTransport()
        transport._pool._network_backend = OverriddenHttpBackend()
        self._client = client

    def cursor(self, **kwargs: Any) -> Cursor:
        if self.closed:
            raise ConnectionClosedError("Unable to create cursor: connection closed.")

        c = self.cursor_type(client=self._client, connection=self, **kwargs)
        self._cursors.append(c)
        return c

    # Context manager support
    async def __aenter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed.")
        return self

    async def aclose(self) -> None:
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

        if self._system_engine_connection:
            await self._system_engine_connection.aclose()

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        await self.aclose()


async def connect(
    auth: Optional[Auth] = None,
    account_name: Optional[str] = None,
    database: Optional[str] = None,
    engine_name: Optional[str] = None,
    engine_url: Optional[str] = None,
    api_endpoint: str = DEFAULT_API_URL,
    additional_parameters: Dict[str, Any] = {},
) -> Connection:
    # auth parameter is optional in function signature
    # but is required to connect.
    # PEP 249 recommends making it kwargs.
    if not auth:
        raise ConfigurationError("auth is required to connect.")

    # Type checks
    assert auth is not None
    user_drivers = additional_parameters.get("user_drivers", [])
    user_clients = additional_parameters.get("user_clients", [])
    user_agent_header = get_user_agent_header(user_drivers, user_clients)
    # Use v2 if auth is ClientCredentials
    # Use v1 if auth is ServiceAccount or UsernamePassword
    version = auth.get_firebolt_version()
    if version == 2:
        assert account_name is not None
        return await connect_v2(
            auth=auth,
            user_agent_header=user_agent_header,
            account_name=account_name,
            database=database,
            engine_name=engine_name,
            api_endpoint=api_endpoint,
        )
    elif version == 1:
        return await connect_v1(
            auth=auth,
            user_agent_header=user_agent_header,
            account_name=account_name,
            database=database,
            engine_name=engine_name,
            engine_url=engine_url,
            api_endpoint=api_endpoint,
        )
    else:
        raise ConfigurationError(f"Unsupported auth type: {type(auth)}")


async def connect_v2(
    auth: Auth,
    user_agent_header: str,
    account_name: Optional[str] = None,
    database: Optional[str] = None,
    engine_name: Optional[str] = None,
    api_endpoint: str = DEFAULT_API_URL,
) -> Connection:
    """Connect to Firebolt.

    Args:
        `auth` (Auth) Authentication object
        `database` (str): Name of the database to connect
        `engine_name` (Optional[str]): Name of the engine to connect to
        `account_name` (Optional[str]): For customers with multiple accounts;
                                        if none, default is used
        `api_endpoint` (str): Firebolt API endpoint. Used for authentication
        `additional_parameters` (Optional[Dict]): Dictionary of less widely-used
                                arguments for connection

    """
    # These parameters are optional in function signature
    # but are required to connect.
    # PEP 249 recommends making them kwargs.
    for name, value in [("account_name", account_name)]:
        if not value:
            raise ConfigurationError(f"{name} is required to connect.")

    # Type checks
    assert auth is not None
    assert account_name is not None

    api_endpoint = fix_url_schema(api_endpoint)

    system_engine_url = fix_url_schema(
        await _get_system_engine_url(auth, account_name, api_endpoint)
    )

    transport = AsyncHTTPTransport()
    transport._pool._network_backend = OverriddenHttpBackend()
    client = AsyncClientV2(
        auth=auth,
        account_name=account_name,
        base_url=system_engine_url,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        transport=transport,
        headers={"User-Agent": user_agent_header},
    )
    # Don't use context manager since this will be stored
    # and used in a resulting connection
    system_engine_connection = Connection(
        system_engine_url,
        database,
        client,
        CursorV2,
        None,
        api_endpoint,
    )
    if not engine_name:
        return system_engine_connection

    else:
        try:
            cursor = system_engine_connection.cursor()
            assert isinstance(cursor, CursorV2)  # Mypy check
            with Timer("[PERFORMANCE] Resolving engine name "):
                (
                    engine_url,
                    status,
                    attached_db,
                ) = await cursor._get_engine_url_status_db(engine_name)

            if status != "Running":
                raise EngineNotRunningError(engine_name)

            if database is not None and database != attached_db:
                raise InterfaceError(
                    f"Engine {engine_name} is attached to {attached_db} "
                    f"instead of {database}"
                )
            elif database is None:
                database = attached_db

            assert engine_url is not None

            engine_url = fix_url_schema(engine_url)
            client = AsyncClientV2(
                auth=auth,
                account_name=account_name,
                base_url=engine_url,
                api_endpoint=api_endpoint,
                timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
                transport=transport,
                headers={"User-Agent": user_agent_header},
            )
            return Connection(
                engine_url,
                database,
                client,
                CursorV2,
                system_engine_connection,
                api_endpoint,
            )
        except:  # noqa
            await system_engine_connection.aclose()
            raise


async def connect_v1(
    auth: Auth,
    user_agent_header: str,
    database: Optional[str] = None,
    account_name: Optional[str] = None,
    engine_name: Optional[str] = None,
    engine_url: Optional[str] = None,
    api_endpoint: str = DEFAULT_API_URL,
) -> Connection:
    # These parameters are optional in function signature
    # but are required to connect.
    # PEP 249 recommends making them kwargs.
    if not database:
        raise ConfigurationError("database name is required to connect.")

    validate_engine_name_and_url_v1(engine_name, engine_url)

    api_endpoint = fix_url_schema(api_endpoint)

    # Override tcp keepalive settings for connection
    transport = AsyncHTTPTransport()
    transport._pool._network_backend = OverriddenHttpBackend()
    no_engine_client = AsyncClientV1(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        transport=transport,
        headers={"User-Agent": user_agent_header},
    )

    # Mypy checks, this should never happen
    assert database is not None

    if not engine_name and not engine_url:
        engine_url = await no_engine_client._get_database_default_engine_url(
            database=database
        )

    elif engine_name:
        engine_url = await no_engine_client._resolve_engine_url(engine_name=engine_name)
    elif account_name:
        # In above if branches account name is validated since it's used to
        # resolve or get an engine url.
        # We need to manually validate account_name if none of the above
        # cases are triggered.
        await no_engine_client.account_id

    assert engine_url is not None

    engine_url = fix_url_schema(engine_url)
    client = AsyncClientV1(
        auth=auth,
        account_name=account_name,
        base_url=engine_url,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        transport=transport,
        headers={"User-Agent": user_agent_header},
    )
    return Connection(engine_url, database, client, CursorV1, None, api_endpoint)
