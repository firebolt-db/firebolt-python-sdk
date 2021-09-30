from __future__ import annotations

from functools import wraps
from inspect import cleandoc
from types import TracebackType
from typing import Any, Callable, List

from firebolt.client import DEFAULT_API_URL, FireboltClient
from firebolt.common.exception import ConnectionClosedError
from firebolt.db.cursor import Cursor


def check_not_closed(func: Callable) -> Callable:
    """(Decorator) ensure cursor is not closed before calling method"""

    @wraps(func)
    def inner(self: Connection, *args: Any, **kwargs: Any) -> Any:
        if self._is_closed:
            raise ConnectionClosedError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


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
    __slots__ = ("_client", "_cursors", "database", "_is_closed")

    def __init__(
        self,
        engine_url: str,
        database: str,  # TODO: Get by engine name
        username: str,
        password: str,
        api_endpoint: str = DEFAULT_API_URL,
    ):
        self._client = FireboltClient(
            auth=(username, password), base_url=engine_url, api_endpoint=api_endpoint
        )
        self.database = database
        self._cursors: List[Cursor] = []
        self._is_closed = False

    @check_not_closed
    def cursor(self) -> Cursor:
        """Create new cursor object."""
        c = Cursor(self._client, self)
        self._cursors.append(c)
        return c

    @check_not_closed
    def close(self) -> None:
        """Close connection and all underlying cursors."""
        # self._cursors is going to be changed during closing cursors
        cursors = self._cursors[:]
        for c in cursors:
            c.close()
        self._client.close()
        self._is_closed = True

    @property
    def closed(self) -> bool:
        """True if connection is closed, False otherwise."""
        return self._is_closed

    def _remove_cursor(self, cursor: Cursor) -> None:
        if cursor in self._cursors:
            self._cursors.remove(cursor)

    # Context manager support
    def __enter__(self) -> Connection:
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()
