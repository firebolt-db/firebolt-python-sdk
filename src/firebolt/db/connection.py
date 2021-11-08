from __future__ import annotations

from inspect import cleandoc
from types import TracebackType

from firebolt.async_db.connection import Connection as AsyncConnection
from firebolt.async_db.connection import async_connect_factory
from firebolt.client import Client
from firebolt.common.exception import ConnectionClosedError
from firebolt.common.util import async_to_sync
from firebolt.db.cursor import Cursor

DEFAULT_TIMEOUT_SECONDS: int = 5


class Connection(AsyncConnection):
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

    client_class = Client
    cursor_class = Cursor

    # Context manager support
    def __enter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed")
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()


connect = async_to_sync(async_connect_factory(Connection))
