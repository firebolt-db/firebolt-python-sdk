from __future__ import annotations

import logging
import socket
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from httpcore.backends.auto import AutoBackend
from httpcore.backends.base import AsyncNetworkStream
from httpx import AsyncHTTPTransport, Timeout, codes

from firebolt.async_db.cursor import Cursor
from firebolt.client import DEFAULT_API_URL, AsyncClient
from firebolt.client.auth import Auth
from firebolt.common.base_connection import BaseConnection
from firebolt.common.settings import (
    DEFAULT_TIMEOUT_SECONDS,
    KEEPALIVE_FLAG,
    KEEPIDLE_RATE,
)
from firebolt.utils.exception import (
    ConfigurationError,
    ConnectionClosedError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME
from firebolt.utils.usage_tracker import get_user_agent_header
from firebolt.utils.util import fix_url_schema

logger = logging.getLogger(__name__)


async def _get_system_engine_url(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> str:
    async with AsyncClient(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        url = GATEWAY_HOST_BY_ACCOUNT_NAME.format(account_name=account_name)
        response = await client.get(url=url)
        if response.status_code != codes.OK:
            raise InterfaceError(
                f"Unable to retrieve system engine endpoint {url}: "
                f"{response.status} {response.content}"
            )
        return response.json()["gatewayHost"]


async def _get_database_default_engine_url(
    system_engine: Connection, database_name: str
) -> str:
    cursor = system_engine.cursor()
    await cursor.execute(
        """
        SELECT engs.engine_url, engs.status
        FROM information_schema.databases AS dbs
        INNER JOIN information_schema.engines AS engs
        ON engs.attached_to = dbs.database_name
        AND engs.engine_name = NULLIF(SPLIT_PART(ARRAY_FIRST(
                eng_name -> eng_name LIKE '%(default)',
                SPLIT(',', attached_engines)
            ), ' ', 1), '')
        WHERE database_name = ?;
        """,
        [database_name],
    )
    row = await cursor.fetchone()
    if row is None:
        raise InterfaceError(f"Database {database_name} doesn't have a default engine")
    engine_url, status = row
    if status != "Running":
        raise InterfaceError(f"A default engine for {database_name} is not running")
    return str(engine_url)  # Mypy check

async def _get_engine_url_and_db(
    system_engine: Connection, engine_name: str
) -> Tuple[str, str]:
    cursor = system_engine.cursor()
    await cursor.execute(
        """
        SELECT engine_url, attached_to, status FROM information_schema.engines
        WHERE engine_name=?
        """,
        [engine_name],
    )
    row = await cursor.fetchone()
    if row is None:
        raise InterfaceError(f"Engine with name {engine_name} doesn't exist")
    engine_url, database, status = row
    if status != "Running":
        raise InterfaceError(f"Engine {engine_name} is not running")
    return str(engine_url), str(database)  # Mypy check

async def connect(
    auth: Optional[Auth] = None,
    account_name: Optional[str] = None,
    database: Optional[str] = None,
    engine_name: Optional[str] = None,
    api_endpoint: str = DEFAULT_API_URL,
    additional_parameters: Dict[str, Any] = {},
) -> Connection:
    """Connect to Firebolt database.

    Args:
        `auth` (Auth) Authentication object.
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
    for name, value in (("auth", auth), ("account_name", account_name)):
        if not value:
            raise ConfigurationError(f"{name} is required to connect.")

    # Type checks
    assert auth is not None
    assert account_name is not None

    api_endpoint = fix_url_schema(api_endpoint)

    if not engine_name and not database:
        # Return system engine connection
        return connection_class(
            system_engine_url, None, auth, api_endpoint, additional_parameters
        )

    else:
        async with Connection(
            system_engine_url, None, auth, api_endpoint, additional_parameters
        ) as system_engine_connection:
            if engine_name:
                engine_url, attached_db = await _get_engine_url_and_db(
                    system_engine_connection, engine_name
                )

                if database is not None and database != attached_db:
                    raise InterfaceError(
                        f"Engine {engine_name} is not attached to {database}, "
                        f"but to {attached_db}"
                    )
                elif database is None:
                    database = attached_db

            elif database:
                # Get database default engine
                engine_url = await _get_database_default_engine_url(
                    system_engine_connection, database
                )

        assert engine_url is not None

        engine_url = fix_url_schema(engine_url)
        return Connection(
            engine_url, database, auth, api_endpoint, additional_parameters
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
        self._cursors: List[Cursor] = []
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
        super().__init__()

    def cursor(self, **kwargs: Any) -> Cursor:
        if self.closed:
            raise ConnectionClosedError("Unable to create cursor: connection closed.")

        c = Cursor(client=self._client, connection=self, **kwargs)
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

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        await self.aclose()
