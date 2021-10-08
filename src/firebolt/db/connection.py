from __future__ import annotations

from inspect import cleandoc
from types import TracebackType
from typing import List

from httpx import Timeout
from readerwriterlock.rwlock import RWLockWrite

from firebolt.client import DEFAULT_API_URL, Client
from firebolt.common.exception import ConnectionClosedError
from firebolt.db.cursor import Cursor

DEFAULT_TIMEOUT_SECONDS: int = 5


class Connection:
    cleandoc(
        """
        Firebolt database connection class. Implements PEP-249.

        Parameters:
            username - Firebolt account username
            password - Firebolt account password
            engine_url - Firebolt database engine REST API url
            api_endpoint(optional) - Firebolt API endpoint. Used for authentication

        Methods:
            cursor - created new Cursor object
            close - close the Connection and all it's cursors

        Firebolt currenly doesn't support transactions so commit and rollback methods
        are not implemented.
        """
    )
    __slots__ = ("_client", "_cursors", "database", "_is_closed", "_closing_lock")

    def __init__(
        self,
        engine_url: str,
        database: str,  # TODO: Get by engine name
        username: str,
        password: str,
        api_endpoint: str = DEFAULT_API_URL,
    ):
        engine_url = (
            engine_url if engine_url.startswith("http") else f"https://{engine_url}"
        )
        self._client = Client(
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

            c = Cursor(self._client, self)
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
            self._client.close()
            self._is_closed = True

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
    def __enter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("Connection is already closed")
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()
