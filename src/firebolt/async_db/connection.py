from __future__ import annotations

from ssl import SSLContext
from types import TracebackType
from typing import Any, Dict, List, Optional, Type, Union

from httpx import Timeout

from firebolt.async_db.cursor import Cursor, CursorV1, CursorV2
from firebolt.async_db.util import _get_system_engine_url_and_params
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import Auth
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.client.client import AsyncClient, AsyncClientV1, AsyncClientV2
from firebolt.common.base_connection import (
    ASYNC_QUERY_CANCEL,
    ASYNC_QUERY_STATUS_REQUEST,
    ASYNC_QUERY_STATUS_RUNNING,
    ASYNC_QUERY_STATUS_SUCCESSFUL,
    AsyncQueryInfo,
    BaseConnection,
    _parse_async_query_info_results,
)
from firebolt.common.cache import _firebolt_system_engine_cache
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    ConfigurationError,
    ConnectionClosedError,
    FireboltError,
)
from firebolt.utils.firebolt_core import (
    get_core_certificate_context,
    parse_firebolt_core_url,
    validate_firebolt_core_parameters,
)
from firebolt.utils.usage_tracker import get_user_agent_header
from firebolt.utils.util import fix_url_schema, validate_engine_name_and_url_v1


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
        "client_class",
        "cursor_type",
    )

    def __init__(
        self,
        engine_url: str,
        database: Optional[str],
        client: AsyncClient,
        cursor_type: Type[Cursor],
        api_endpoint: str,
        init_parameters: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(cursor_type)
        self.api_endpoint = api_endpoint
        self.engine_url = engine_url
        self._cursors: List[Cursor] = []
        self._client = client
        self.init_parameters = init_parameters or {}
        if database:
            self.init_parameters["database"] = database

    def cursor(self, **kwargs: Any) -> Cursor:
        if self.closed:
            raise ConnectionClosedError("Unable to create cursor: connection closed.")

        c = self.cursor_type(client=self._client, connection=self, **kwargs)
        self._cursors.append(c)
        return c

    # Server-side async methods
    async def get_async_query_info(self, token: str) -> List[AsyncQueryInfo]:
        """
        Retrieve information about an asynchronous query using its token.
        This method fetches the status and details of an asynchronous query
        identified by the provided token.
        Args:
            token (str): The token identifying the asynchronous query.
        Returns:
            List[AsyncQueryInfo]: A list of AsyncQueryInfo objects containing
            details about the asynchronous query.
        """

        if self.cursor_type != CursorV2:
            raise FireboltError(
                "This method is only supported for connection with service account."
            )
        cursor = self.cursor()
        await cursor.execute(ASYNC_QUERY_STATUS_REQUEST, [token])
        results = await cursor.fetchall()
        if not results:
            raise FireboltError("Unexpected result from async query status request.")
        columns = cursor.description
        columns_names = [column.name for column in columns]
        return _parse_async_query_info_results(results, columns_names)

    def _raise_if_multiple_async_results(
        self, async_query_info: List[AsyncQueryInfo]
    ) -> None:
        # We expect only one result in current implementation
        if len(async_query_info) != 1:
            raise NotImplementedError(
                "Async query status request returned more than one result. "
                "This is not supported yet."
            )

    async def is_async_query_running(self, token: str) -> bool:
        """
        Check if an async query is still running.

        Args:
            token: Async query token. Can be obtained from Cursor.async_query_token.

        Returns:
            bool: True if async query is still running, False otherwise
        """
        async_query_info = await self.get_async_query_info(token)
        self._raise_if_multiple_async_results(async_query_info)
        # We expect only one result
        return async_query_info[0].status == ASYNC_QUERY_STATUS_RUNNING

    async def is_async_query_successful(self, token: str) -> Optional[bool]:
        """
        Check if an async query has finished and was successful.

        Args:
            token: Async query token. Can be obtained from Cursor.async_query_token.

        Returns:
            bool: None if the query is still running, True if successful,
                  False otherwise
        """
        async_query_info_list = await self.get_async_query_info(token)
        self._raise_if_multiple_async_results(async_query_info_list)
        async_query_info = async_query_info_list[0]
        if async_query_info.status == ASYNC_QUERY_STATUS_RUNNING:
            return None
        return async_query_info.status == ASYNC_QUERY_STATUS_SUCCESSFUL

    async def cancel_async_query(self, token: str) -> None:
        """
        Cancel an async query.

        Args:
            token: Async query token. Can be obtained from Cursor.async_query_token.
        """
        async_query_info = await self.get_async_query_info(token)
        self._raise_if_multiple_async_results(async_query_info)
        cursor = self.cursor()
        await cursor.execute(ASYNC_QUERY_CANCEL, [async_query_info[0].query_id])

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
            await c.aclose()
        await self._client.aclose()
        self._is_closed = True

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
    disable_cache: bool = False,
    url: Optional[str] = None,
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
    if disable_cache:
        _firebolt_system_engine_cache.disable()
    # Use CORE if auth is FireboltCore
    # Use V2 if auth is ClientCredentials
    # Use V1 if auth is ServiceAccount or UsernamePassword
    auth_version = auth.get_firebolt_version()
    if auth_version == FireboltAuthVersion.CORE:
        # Verify that Core-incompatible parameters are not provided
        validate_firebolt_core_parameters(account_name, engine_name, engine_url)
        return connect_core(
            auth=auth,
            user_agent_header=user_agent_header,
            database=database,
            connection_url=url,
        )
    elif auth_version == FireboltAuthVersion.V2:
        assert account_name is not None
        return await connect_v2(
            auth=auth,
            user_agent_header=user_agent_header,
            account_name=account_name,
            database=database,
            engine_name=engine_name,
            api_endpoint=api_endpoint,
        )
    elif auth_version == FireboltAuthVersion.V1:
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

    system_engine_url, system_engine_params = await _get_system_engine_url_and_params(
        auth, account_name, api_endpoint
    )

    client = AsyncClientV2(
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        headers={"User-Agent": user_agent_header},
    )

    async with Connection(
        system_engine_url,
        None,
        client,
        CursorV2,
        api_endpoint,
        system_engine_params,
    ) as system_engine_connection:

        cursor = system_engine_connection.cursor()
        if database:
            await cursor.execute(f'USE DATABASE "{database}"')
        if engine_name:
            await cursor.execute(f'USE ENGINE "{engine_name}"')
        # Ensure cursors created from this connection are using the same starting
        # database and engine
        return Connection(
            cursor.engine_url,
            cursor.database,
            client.clone(),
            CursorV2,
            api_endpoint,
            cursor.parameters,
        )


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

    no_engine_client = AsyncClientV1(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
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
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        headers={"User-Agent": user_agent_header},
    )
    return Connection(engine_url, database, client, CursorV1, api_endpoint)


def connect_core(
    auth: Auth,
    user_agent_header: str,
    database: Optional[str] = None,
    connection_url: Optional[str] = None,
) -> Connection:
    """Connect to Firebolt Core.

    Args:
        auth (Auth): Authentication object (must be FireboltCore)
        user_agent_header (str): User agent header string
        database (Optional[str]): Name of the database to connect to
            (defaults to 'firebolt')
        connection_url (Optional[str]): URL in format protocol://host:port
            Protocol defaults to http, host defaults to localhost, port
            defaults to 3473.

    Returns:
        Connection: A connection to Firebolt Core
    """
    connection_params = parse_firebolt_core_url(connection_url)

    ctx: Union[SSLContext, bool] = True  # Default context
    if connection_params.scheme == "https":
        ctx = get_core_certificate_context()

    verified_url = connection_params.geturl()
    client = AsyncClientV2(
        auth=auth,
        account_name="",  # FireboltCore does not require an account name
        base_url=verified_url,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        headers={"User-Agent": user_agent_header},
        verify=ctx,
    )

    return Connection(
        engine_url=verified_url,
        database=database,
        client=client,
        cursor_type=CursorV2,
        api_endpoint=verified_url,
    )
