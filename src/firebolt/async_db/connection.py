from __future__ import annotations

from inspect import cleandoc
from json import JSONDecodeError
from types import TracebackType
from typing import Callable, List, Optional, Type

from httpx import HTTPStatusError, RequestError, Timeout
from readerwriterlock.rwlock import RWLockWrite

from firebolt.async_db.cursor import Cursor
from firebolt.client import DEFAULT_API_URL, AsyncClient
from firebolt.common.exception import ConnectionClosedError, InterfaceError

DEFAULT_TIMEOUT_SECONDS: int = 5


async def _resolve_engine_url(
    engine_name: str, username: str, password: str, api_endpoint: str
) -> str:
    api_endpoint = (
        api_endpoint if api_endpoint.startswith("http") else f"https://{api_endpoint}"
    )
    async with AsyncClient(
        auth=(username, password),
        base_url=api_endpoint,
        api_endpoint=api_endpoint,
    ) as client:
        try:
            response = await client.get(
                url="/core/v1/account/engines:getIdByName",
                params={"engine_name": engine_name},
            )
            response.raise_for_status()
            engine_id = response.json()["engine_id"]["engine_id"]
            account_id = await client.account_id
            response = await client.get(
                url=f"/core/v1/accounts/{account_id}/engines/{engine_id}",
            )
            response.raise_for_status()
            return response.json()["engine"]["endpoint"]
        except (JSONDecodeError, RequestError, HTTPStatusError, RuntimeError) as e:
            raise InterfaceError(f"unable to retrieve engine endpoint: {e}")


def connect_factory(connection_class: Type) -> Callable:
    async def connect_inner(
        database: str = None,
        username: str = None,
        password: str = None,
        engine_name: Optional[str] = None,
        engine_url: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
    ) -> Connection:
        cleandoc(
            """
            Connect to Firebolt database.

            Connection parameters:
            database - name of the database to connect
            username - user name to use for authentication
            password - password to use for authentication
            engine_name - name of the engine to connect to
            engine_url - engine endpoint to use
            note: either engine_name or engine_url should be provided, but not both
            """
        )

        if engine_name and engine_url:
            raise InterfaceError(
                "Both engine_name and engine_url are provided."
                "Provide only one to connect."
            )
        if not engine_name and not engine_url:
            raise InterfaceError(
                "Neither engine_name nor engine_url are provided."
                "Provide one to connect."
            )
        # This parameters are optional in function signature,
        # but are required to connect.
        # It's recomended to make them kwargs by PEP 249
        for param, name in (
            (database, "database"),
            (username, "username"),
            (password, "password"),
        ):
            if not param:
                raise InterfaceError(f"{name} is required to connect.")

            if engine_name is not None:
                engine_url = await _resolve_engine_url(
                    engine_name,
                    username,
                    password,
                    api_endpoint,
                )

        # Mypy checks, this should never happen
        assert engine_url is not None
        assert database is not None
        assert username is not None
        assert password is not None

        engine_url = (
            engine_url if engine_url.startswith("http") else f"https://{engine_url}"
        )
        return connection_class(engine_url, database, username, password, api_endpoint)

    return connect_inner


class Connection:
    cleandoc(
        """
        Firebolt database connection class. Implements PEP-249.

        Parameters:
            engine_url - Firebolt database engine REST API url
            database - Firebolt database name
            username - Firebolt account username
            password - Firebolt account password
            api_endpoint(optional) - Firebolt API endpoint. Used for authentication

        Methods:
            cursor - create new Cursor object
            close - close the Connection and all it's cursors

        Firebolt currenly doesn't support transactions so commit and rollback methods
        are not implemented.
        """
    )
    client_class = AsyncClient
    cursor_class = Cursor
    __slots__ = ("_client", "_cursors", "database", "_is_closed", "_closing_lock")

    def __init__(
        self,
        engine_url: str,
        database: str,  # TODO: Get by engine name
        username: str,
        password: str,
        api_endpoint: str = DEFAULT_API_URL,
    ):
        self._client = self.client_class(
            auth=(username, password),
            base_url=engine_url,
            api_endpoint=api_endpoint,
            timeout=Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        )
        self.database = database
        self._cursors: List[Cursor] = []
        self._is_closed = False
        # Holding this lock for write means that connection is closing itself.
        # cursor() should hold this lock for read to read/write state
        self._closing_lock = RWLockWrite()

    def cursor(self) -> Cursor:
        """Create new cursor object."""
        with self._closing_lock.gen_rlock():
            if self.closed:
                raise ConnectionClosedError(
                    "Unable to create cursor: connection closed"
                )

            c = self.cursor_class(self._client, self)
            self._cursors.append(c)
            return c

    def close(self) -> None:
        """Close connection and all underlying cursors."""
        with self._closing_lock.gen_wlock():
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
            self._is_closed = True

    async def close_client(self) -> None:
        await self._client.aclose()

    @property
    def closed(self) -> bool:
        """True if connection is closed, False otherwise."""
        return self._is_closed

    def _remove_cursor(self, cursor: Cursor) -> None:
        # This way it's atomic
        try:
            self._cursors.remove(cursor)
        except ValueError:
            pass

    # Context manager support
    async def __aenter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed")
        return self

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()
        await self.close_client()

    def __del__(self):
        self.close()


connect = connect_factory(Connection)
