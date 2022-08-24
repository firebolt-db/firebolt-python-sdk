from __future__ import annotations

from functools import wraps
from types import TracebackType
from typing import Any
from warnings import warn

from readerwriterlock.rwlock import RWLockWrite

from firebolt.async_db.connection import BaseConnection as AsyncBaseConnection
from firebolt.async_db.connection import async_connect_factory
from firebolt.db.cursor import Cursor
from firebolt.utils.exception import ConnectionClosedError
from firebolt.utils.util import AsyncJobThread, async_to_sync


class Connection(AsyncBaseConnection):
    """
    Firebolt database connection class. Implements PEP-249.

    Args:

        engine_url: Firebolt database engine REST API url
        database: Firebolt database name
        username: Firebolt account username
        password: Firebolt account password
        api_endpoint: Optional. Firebolt API endpoint. Used for authentication.

    Note:
        Firebolt currenly doesn't support transactions so commit and rollback methods
        are not implemented.
    """

    __slots__ = AsyncBaseConnection.__slots__ + ("_closing_lock", "_async_job_thread")

    cursor_class = Cursor

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Holding this lock for write means that connection is closing itself.
        # cursor() should hold this lock for read to read/write state
        self._closing_lock = RWLockWrite()
        self._async_job_thread = AsyncJobThread()

    def cursor(self) -> Cursor:
        with self._closing_lock.gen_rlock():
            c = super()._cursor(async_job_thread=self._async_job_thread)
            assert isinstance(c, Cursor)  # typecheck
            return c

    @wraps(AsyncBaseConnection._aclose)
    def close(self) -> None:
        with self._closing_lock.gen_wlock():
            async_to_sync(self._aclose, self._async_job_thread)()

    # Context manager support
    def __enter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed.")
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if not self.closed:
            warn(f"Unclosed {self!r}", UserWarning)


connect = async_to_sync(async_connect_factory(Connection))
