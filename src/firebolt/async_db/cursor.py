from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from functools import wraps
from inspect import cleandoc
from json import JSONDecodeError
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from aiorwlock import RWLock
from httpx import Response, codes

from firebolt.async_db._types import (
    ColType,
    Column,
    RawColType,
    parse_type,
    parse_value,
)
from firebolt.client import AsyncClient
from firebolt.common.exception import (
    CursorClosedError,
    DataError,
    OperationalError,
    ProgrammingError,
    QueryNotRunError,
)

if TYPE_CHECKING:
    from firebolt.async_db.connection import Connection

ParameterType = Union[int, float, str, datetime, date, bool, Sequence]

JSON_OUTPUT_FORMAT = "JSONCompact"


class CursorState(Enum):
    NONE = 1
    DONE = 3
    CLOSED = 4


def check_not_closed(func: Callable) -> Callable:
    """(Decorator) ensure cursor is not closed before calling method."""

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
        calling cursor method.
        """
    )

    @wraps(func)
    def inner(self: Cursor, *args: Any, **kwargs: Any) -> Any:
        if self._state == CursorState.NONE:
            raise QueryNotRunError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


class BaseCursor:
    __slots__ = (
        "connection",
        "_arraysize",
        "_client",
        "_state",
        "_descriptions",
        "_rowcount",
        "_rows",
        "_idx",
        "_idx_lock",
    )

    default_arraysize = 1

    def __init__(self, client: AsyncClient, connection: Connection):
        self.connection = connection
        self._client = client
        self._arraysize = self.default_arraysize
        self._rows: Optional[List[List[RawColType]]] = None
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
        """The number of rows produced by last query."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Default number of rows returned by fetchmany."""
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
        """True if connection is closed, False otherwise."""
        return self._state == CursorState.CLOSED

    def close(self) -> None:
        """Terminate an ongoing query (if any) and mark connection as closed."""
        self._state = CursorState.CLOSED
        # remove typecheck skip  after connection is implemented
        self.connection._remove_cursor(self)  # type: ignore

    def _store_query_data(self, response: Response) -> None:
        """Store information about executed query from httpx response."""
        # Empty response is returned for insert query
        if response.headers.get("content-length", "") == "0":
            return
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
            raise DataError(f"Invalid query data format: {str(err)}")

    def _raise_if_error(self, resp: Response) -> None:
        """Raise a proper error if any"""
        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise OperationalError(
                f"Error executing query:\n{resp.read().decode('utf-8')}"
            )
        if resp.status_code == codes.FORBIDDEN:
            raise ProgrammingError(resp.read().decode("utf-8"))
        resp.raise_for_status()

    def _reset(self) -> None:
        """Clear all data stored from previous query."""
        self._state = CursorState.NONE
        self._rows = None
        self._descriptions = None
        self._rowcount = -1
        self._idx = 0

    async def _do_execute_request(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        set_parameters: Optional[Dict] = None,
    ) -> Response:
        resp = await self._client.request(
            url="/",
            method="POST",
            params={
                "database": self.connection.database,
                "output_format": JSON_OUTPUT_FORMAT,
                **(set_parameters or dict()),
            },
            content=query,
        )

        self._raise_if_error(resp)
        return resp

    @check_not_closed
    async def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        set_parameters: Optional[Dict] = None,
    ) -> int:
        """Prepare and execute a database query. Return row count."""
        self._reset()
        resp = await self._do_execute_request(query, parameters, set_parameters)
        self._store_query_data(resp)
        self._state = CursorState.DONE
        return self.rowcount

    @check_not_closed
    async def executemany(
        self, query: str, parameters_seq: Sequence[Sequence[ParameterType]]
    ) -> int:
        cleandoc(
            """
            Prepare and execute a database query against all parameter
            sequences provided. Return last query row count.
            """
        )
        self._reset()
        resp = None
        for parameters in parameters_seq:
            resp = await self._do_execute_request(query, parameters)
            if resp is not None:
                self._store_query_data(resp)
                self._state = CursorState.DONE
        return self.rowcount

    def _parse_row(self, row: List[RawColType]) -> List[ColType]:
        """Parse a single data row based on query column types"""
        assert len(row) == len(self.description)
        return [
            parse_value(col, self.description[i].type_code) for i, col in enumerate(row)
        ]

    def _get_next_range(self, size: int) -> Tuple[int, int]:
        cleandoc(
            """
            Return range of next rows of size (if possible),
            and update _idx to point to the end of this range
            """
        )
        if self._rows is None:
            # No elements to take
            return (0, 0)
        left = self._idx
        right = min(self._idx + size, len(self._rows))
        self._idx = right
        return left, right

    @check_not_closed
    @check_query_executed
    def fetchone(self) -> Optional[List[ColType]]:
        """Fetch the next row of a query result set."""
        left, right = self._get_next_range(1)
        if left == right:
            # We are out of elements
            return None
        assert self._rows is not None
        return self._parse_row(self._rows[left])

    @check_not_closed
    @check_query_executed
    def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        cleandoc(
            """
            Fetch the next set of rows of a query result,
            cursor.arraysize is default size.
            """
        )
        size = size if size is not None else self.arraysize
        left, right = self._get_next_range(size)
        assert self._rows is not None
        rows = self._rows[left:right]
        return [self._parse_row(row) for row in rows]

    @check_not_closed
    @check_query_executed
    def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result."""
        assert self._rows is not None
        left, right = self._get_next_range(len(self._rows))
        rows = self._rows[left:right]
        return [self._parse_row(row) for row in rows]

    @check_not_closed
    def setinputsizes(self, sizes: List[int]) -> None:
        """Predefine memory areas for query parameters (does nothing)."""

    @check_not_closed
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns (does nothing)."""

    # Context manager support
    @check_not_closed
    def __enter__(self) -> BaseCursor:
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()


class Cursor(BaseCursor):
    cleandoc(
        """
        Class, responsible for executing asyncio queries to Firebolt Database.
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

    __slots__ = BaseCursor.__slots__ + ("_async_query_lock",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._async_query_lock = RWLock()
        super().__init__(*args, **kwargs)

    @wraps(BaseCursor.execute)
    async def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        set_parameters: Optional[Dict] = None,
    ) -> int:
        async with self._async_query_lock.writer:
            return await super().execute(query, parameters, set_parameters)

    @wraps(BaseCursor.executemany)
    async def executemany(
        self, query: str, parameters_seq: Sequence[Sequence[ParameterType]]
    ) -> int:
        async with self._async_query_lock.writer:
            return await super().executemany(query, parameters_seq)

    @wraps(BaseCursor.fetchone)
    async def fetchone(self) -> Optional[List[ColType]]:
        async with self._async_query_lock.reader:
            return super().fetchone()

    @wraps(BaseCursor.fetchmany)
    async def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        async with self._async_query_lock.reader:
            return super().fetchmany(size)

    @wraps(BaseCursor.fetchall)
    async def fetchall(self) -> List[List[ColType]]:
        async with self._async_query_lock.reader:
            return super().fetchall()

    # Iteration support
    @check_not_closed
    @check_query_executed
    def __aiter__(self) -> Cursor:
        return self

    @check_not_closed
    @check_query_executed
    async def __anext__(self) -> List[ColType]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row
