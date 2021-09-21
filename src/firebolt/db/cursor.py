from collections import namedtuple
from datetime import date, datetime
from enum import Enum
from functools import wraps
from inspect import cleandoc
from json import JSONDecodeError
from typing import Any, Callable, Generator, List, Optional, Sequence, Union

from httpx import Response, codes

from firebolt.client import FireboltClient
from firebolt.common.exception import (
    CursorClosedError,
    QueryError,
    QueryNotRunError,
)

ParameterType = Union[int, float, str, datetime, date, bool, Sequence]
ColType = Union[int, float, str, datetime, date, bool, List]
JSON_OUTPUT_FORMAT = "FB_JSONCompactLimited"


class CursorState(Enum):
    NONE = 1
    DONE = 3
    CLOSED = 4


def check_closed(func: Callable) -> Callable:
    @wraps(func)
    def inner(self: Cursor, *args, **kwargs) -> Any:
        if self.closed:
            raise CursorClosedError(method_name=func.__name__)
        return func()

    return inner


def check_query(func: Callable) -> Callable:
    @wraps(func)
    def inner(self: Cursor, *args, **kwargs) -> Any:
        if self._state == CursorState.NONE:
            raise QueryNotRunError(method_name=func.__name__)
        return func()

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
        "arraysize",
        "_client",
        "_state",
        "_descriptions",
        "_rowcount",
        "_rows",
        "_idx",
    )

    default_arraysize = 1

    def __init__(self, client: FireboltClient, connection):
        self.connection = connection
        self._client = client
        self.arraysize = self.default_arraysize
        self._reset()

    def __del__(self):
        self.close()

    @property
    @check_closed
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

    @property
    @check_closed
    @check_query
    def rowcount(self):
        "The number of rows produced by last query"
        return self._rowcount

    @property
    def closed(self):
        "True if connection is closed, False otherwise"
        return self._state == CursorState.CLOSED

    @check_closed
    def close(self):
        "Terminate an ongoing query (if any) and mark connection as closed"
        self._state = CursorState.CLOSED

    def _store_query_data(self, response: Response):
        "Store information about executed query from httpx response"
        try:
            query_data = response.json()
            # TODO: Convert columns based on type info
            self._rows = query_data["data"]
            self._rowcount = int(query_data["rows"])
            # TODO: Convert type names to type codes
            self._descriptions = [
                Column(name=d["name"], type_code=d["type"]) for d in query_data["meta"]
            ]
        except (KeyError, JSONDecodeError) as err:
            raise QueryError(f"Invalid query data format: {str(err)}")

    def _reset(self):
        "Clear all data stored from previous query"
        self._state = CursorState.NONE
        self._rows: List = None
        self._descriptions: List = None
        self._rowcount = 0
        self._idx = 0

    @check_closed
    def execute(
        self, query: str, parameters: Optional[Sequence[ParameterType]] = None
    ) -> int:
        "Prepare and execute a database query. Return row count"
        self._reset()

        resp = self._client.request(
            method="POST",
            params={
                "database": self.connection.database,
                "output_format": JSON_OUTPUT_FORMAT,
            },
            content=query,
        )

        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise QueryError(f"Error executing query:\n{resp.read()}")

        resp.raise_for_status()

        self._store_query_data(resp)
        self._state = CursorState.DONE
        return self.rowcount

    @check_closed
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
            rc = self.execute()
        return rc

    @check_closed
    @check_query
    def fetchone(self) -> List[ColType]:
        "Fetch the next row of a query result set"
        if self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            return row
        return None

    @check_closed
    @check_query
    def fetchmany(self, size: Optional[int] = None) -> List[ColType]:
        "Fetch the next set of rows of a query result, cursor.arraysize is default size"
        size = size or self.arraysize
        if self._idx < len(self._rows):
            right = min(self._idx + size, len(self._rows))
            rows = self._rows[self._idx : right]
            self._idx = right
            return rows
        return []

    @check_closed
    @check_query
    def fetchall(self) -> List[ColType]:
        "Fetch all remaining rows of a query result"
        if self._idx < len(self._rows):
            rows = self._rows[self._idx]
            self._idx = len(self._rows)
            return rows
        return []

    @check_closed
    def setinputsizes(self, sizes: List[int]):
        "Predefine memory areas for query parameters (does nothing)"

    @check_closed
    def setoutputsize(self, size: int, column: Optional[int] = None):
        "Set a column buffer size for fetches of large columns (does nothing)"

    # Iteration support
    @check_closed
    @check_query
    def __iter__(self) -> Generator[List[ColType], None, None]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    # Context manager support
    @check_closed
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
