from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime
from enum import Enum
from functools import wraps
from inspect import cleandoc
from json import JSONDecodeError
from types import TracebackType
from typing import Any, Callable, Generator, List, Optional, Sequence, Union

from httpx import Response, codes

from firebolt.client import FireboltClient
from firebolt.common.exception import (
    CursorClosedError,
    QueryError,
    QueryNotRunError,
)
from firebolt.db.typing import ColType, parse_type, parse_value

ParameterType = Union[int, float, str, datetime, date, bool, Sequence]

JSON_OUTPUT_FORMAT = "FB_JSONCompactLimited"


class CursorState(Enum):
    NONE = 1
    DONE = 3
    CLOSED = 4


def check_not_closed(func: Callable) -> Callable:
    """(Decorator) ensure cursor is not closed before calling method"""

    @wraps(func)
    def inner(self: Cursor, *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise CursorClosedError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


def check_query_executed(func: Callable) -> Callable:
    cleandoc(
        """
        (Decorator) ensure that some query has been executed before
        calling cursor method
        """
    )

    @wraps(func)
    def inner(self: Cursor, *args: Any, **kwargs: Any) -> Any:
        if self._state == CursorState.NONE:
            raise QueryNotRunError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


Column = namedtuple(
    "Column",
    (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    ),
)


class Cursor:
    cleandoc(
        """
        Class, responsible for executing queries to Firebolt Database.
        Should not be created directly, use connection.cursor()

        Properties:
        - description - information about a single result row
        - rowcount - the number of rows produced by last query
        - closed - True if connection is closed, False otherwise
        - arraysize - Read/Write, specifies the number of rows to fetch at a time
        with .fetchmany method

        Methods:
        - close - terminate an ongoing query (if any) and mark connection as closed
        - execute - prepare and execute a database query
        - executemany - prepare and execute a database query against all parameter
          sequences provided
        - fetchone - fetch the next row of a query result set
        - fetchmany - fetch the next set of rows of a query result,
          size is cursor.arraysize by default
        - fetchall - fetch all remaining rows of a query result
        - setinputsizes - predefine memory areas for query parameters (does nothing)
        - setoutputsize - set a column buffer size for fetches of large columns
          (does nothing)
        """
    )

    __slots__ = (
        "connection",
        "_arraysize",
        "_client",
        "_state",
        "_descriptions",
        "_rowcount",
        "_rows",
        "_idx",
    )

    default_arraysize = 1

    def __init__(self, client: FireboltClient, connection: Any):
        self.connection = connection
        self._client = client
        self._arraysize = self.default_arraysize
        self._rows: Optional[List[List[ColType]]] = None
        self._descriptions: Optional[List[Column]] = None
        self._reset()

    def __del__(self) -> None:
        self.close()

    @property  # type: ignore
    @check_not_closed
    def description(self) -> Optional[List[Column]]:
        cleandoc(
            """
            Provides information about a single result row of a query
            Attributes:
            - name
            - type_code
            - display_size
            - internal_size
            - precision
            - scale
            - null_ok
            """
        )
        return self._descriptions

    @property  # type: ignore
    @check_not_closed
    def rowcount(self) -> int:
        """The number of rows produced by last query"""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Default number of rows returned by fetchmany"""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError(
                "Invalid arraysize value type, expected int,"
                f" got {type(value).__name__}"
            )
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """True if connection is closed, False otherwise"""
        return self._state == CursorState.CLOSED

    def close(self) -> None:
        """Terminate an ongoing query (if any) and mark connection as closed"""
        self._state = CursorState.CLOSED

    def _store_query_data(self, response: Response) -> None:
        """Store information about executed query from httpx response"""
        try:
            query_data = response.json()
            self._rowcount = int(query_data["rows"])
            self._descriptions = [
                Column(d["name"], parse_type(d["type"]), None, None, None, None, None)
                for d in query_data["meta"]
            ]

            # Parse data during fetch
            self._rows = query_data["data"]
        except (KeyError, JSONDecodeError) as err:
            raise QueryError(f"Invalid query data format: {str(err)}")

    def _reset(self) -> None:
        """Clear all data stored from previous query"""
        self._state = CursorState.NONE
        self._rows = None
        self._descriptions = None
        self._rowcount = -1
        self._idx = 0

    @check_not_closed
    def execute(
        self, query: str, parameters: Optional[Sequence[ParameterType]] = None
    ) -> int:
        """Prepare and execute a database query. Return row count"""
        self._reset()

        resp = self._client.request(
            url="/",
            method="POST",
            params={
                "database": self.connection.database,
                "output_format": JSON_OUTPUT_FORMAT,
            },
            content=query,
        )

        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise QueryError(f"Error executing query:\n{resp.read().decode('utf-8')}")

        resp.raise_for_status()

        self._store_query_data(resp)
        self._state = CursorState.DONE
        return self.rowcount

    @check_not_closed
    def executemany(
        self, query: str, parameters_seq: Sequence[Sequence[ParameterType]]
    ) -> int:
        cleandoc(
            """
            Prepare and execute a database query against all parameter
            sequences provided. Return last query row count
            """
        )
        rc = 0
        for params in parameters_seq:
            rc = self.execute(query, params)
        return rc

    def _parse_row(self, row: List[ColType]) -> List[ColType]:
        """Parse a single data row based on query column types"""
        assert len(row) == len(self.description)
        return [
            parse_value(col, self.description[i].type_code) for i, col in enumerate(row)
        ]

    @check_not_closed
    @check_query_executed
    def fetchone(self) -> Optional[List[ColType]]:
        """Fetch the next row of a query result set"""
        assert self._rows is not None
        if self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            return self._parse_row(row)
        return None

    @check_not_closed
    @check_query_executed
    def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        cleandoc(
            """
            Fetch the next set of rows of a query result,
            cursor.arraysize is default size
            """
        )
        assert self._rows is not None
        size = size or self.arraysize
        if self._idx < len(self._rows):
            right = min(self._idx + size, len(self._rows))
            rows = self._rows[self._idx : right]
            self._idx = right
            return [self._parse_row(row) for row in rows]
        return []

    @check_not_closed
    @check_query_executed
    def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result"""
        assert self._rows is not None
        if self._idx < len(self._rows):
            rows = self._rows[self._idx :]
            self._idx = len(self._rows)
            return [self._parse_row(row) for row in rows]
        return []

    @check_not_closed
    def setinputsizes(self, sizes: List[int]) -> None:
        """Predefine memory areas for query parameters (does nothing)"""

    @check_not_closed
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns (does nothing)"""

    # Iteration support
    @check_not_closed
    @check_query_executed
    def __iter__(self) -> Generator[List[ColType], None, None]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    # Context manager support
    @check_not_closed
    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()
