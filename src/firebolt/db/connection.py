from __future__ import annotations

from functools import wraps
from inspect import cleandoc
from types import TracebackType
from typing import Any

from readerwriterlock.rwlock import RWLockWrite

from firebolt.async_db.connection import BaseConnection as AsyncBaseConnection
from firebolt.async_db.connection import async_connect_factory
from firebolt.common.exception import ConnectionClosedError
from firebolt.common.util import async_to_sync
from firebolt.db.cursor import Cursor

DEFAULT_TIMEOUT_SECONDS: int = 5


class Connection(AsyncBaseConnection):
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

    __slots__ = AsyncBaseConnection.__slots__ + ("_closing_lock",)

    cursor_class = Cursor

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Holding this lock for write means that connection is closing itself.
        # cursor() should hold this lock for read to read/write state
        self._closing_lock = RWLockWrite()

    @wraps(AsyncBaseConnection.cursor)
    def cursor(self) -> Cursor:
        with self._closing_lock.gen_rlock():
            c = super().cursor()
            assert isinstance(c, Cursor)  # typecheck
            return c

    @wraps(AsyncBaseConnection._aclose)
    def close(self) -> None:
        with self._closing_lock.gen_wlock():
            return async_to_sync(super()._aclose)()

    # Context manager support
    def __enter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed")
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


connect = async_to_sync(async_connect_factory(Connection))
