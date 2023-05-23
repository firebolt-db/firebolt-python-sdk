from __future__ import annotations

import logging
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from httpx import Response
from pydantic import BaseModel

from firebolt.common._types import (
    ColType,
    Column,
    ParameterType,
    RawColType,
    SetParameter,
    parse_type,
    parse_value,
)
from firebolt.utils.exception import (
    AsyncExecutionUnavailableError,
    CursorClosedError,
    DataError,
    QueryNotRunError,
)

logger = logging.getLogger(__name__)


JSON_OUTPUT_FORMAT = "JSON_Compact"


class CursorState(Enum):
    NONE = 1
    ERROR = 2
    DONE = 3
    CLOSED = 4


class QueryStatus(Enum):
    """Enumeration of query responses on server-side async queries."""

    RUNNING = 1
    ENDED_SUCCESSFULLY = 2
    ENDED_UNSUCCESSFULLY = 3
    NOT_READY = 4
    STARTED_EXECUTION = 5
    PARSE_ERROR = 6
    CANCELED_EXECUTION = 7
    EXECUTION_ERROR = 8


class Statistics(BaseModel):
    """
    Class for query execution statistics.
    """

    elapsed: float
    rows_read: int
    bytes_read: int
    time_before_execution: float
    time_to_execute: float
    scanned_bytes_cache: Optional[float]
    scanned_bytes_storage: Optional[float]


def check_not_closed(func: Callable) -> Callable:
    """(Decorator) ensure cursor is not closed before calling method."""

    @wraps(func)
    def inner(self: BaseCursor, *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise CursorClosedError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


def check_query_executed(func: Callable) -> Callable:
    """
    (Decorator) ensure that some query has been executed before
    calling cursor method.
    """

    @wraps(func)
    def inner(self: BaseCursor, *args: Any, **kwargs: Any) -> Any:
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
        "_statistics",
        "_rowcount",
        "_rows",
        "_idx",
        "_idx_lock",
        "_row_sets",
        "_next_set_idx",
        "_set_parameters",
        "_query_id",
    )

    default_arraysize = 1

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._arraysize = self.default_arraysize
        # These fields initialized here for type annotations purpose
        self._rows: Optional[List[List[RawColType]]] = None
        self._descriptions: Optional[List[Column]] = None
        self._statistics: Optional[Statistics] = None
        self._row_sets: List[
            Tuple[
                int,
                Optional[List[Column]],
                Optional[Statistics],
                Optional[List[List[RawColType]]],
            ]
        ] = []
        self._set_parameters: Dict[str, Any] = dict()
        self._rowcount = -1
        self._idx = 0
        self._next_set_idx = 0
        self._query_id = ""
        self._reset()

    @property  # type: ignore
    @check_not_closed
    def description(self) -> Optional[List[Column]]:
        """
        Provides information about a single result row of a query.

        Attributes:
            * ``name``
            * ``type_code``
            * ``display_size``
            * ``internal_size``
            * ``precision``
            * ``scale``
            * ``null_ok``
        """
        return self._descriptions

    @property  # type: ignore
    @check_not_closed
    def statistics(self) -> Optional[Statistics]:
        """Query execution statistics returned by the backend."""
        return self._statistics

    @property  # type: ignore
    @check_not_closed
    def rowcount(self) -> int:
        """The number of rows produced by last query."""
        return self._rowcount

    @property  # type: ignore
    @check_not_closed
    def query_id(self) -> str:
        """The query id of a query executed asynchronously."""
        return self._query_id

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

    @check_not_closed
    @check_query_executed
    def nextset(self) -> Optional[bool]:
        """
        Skip to the next available set, discarding any remaining rows
        from the current set.
        Returns True if operation was successful;
        None if there are no more sets to retrive.
        """
        return self._pop_next_set()

    def _pop_next_set(self) -> Optional[bool]:
        """
        Same functionality as .nextset, but doesn't check that query has been executed.
        """
        if self._next_set_idx >= len(self._row_sets):
            return None
        (
            self._rowcount,
            self._descriptions,
            self._statistics,
            self._rows,
        ) = self._row_sets[self._next_set_idx]
        self._idx = 0
        self._next_set_idx += 1
        return True

    def flush_parameters(self) -> None:
        """Cleanup all previously set parameters"""
        self._set_parameters = dict()

    def _reset(self) -> None:
        """Clear all data stored from previous query."""
        self._state = CursorState.NONE
        self._rows = None
        self._descriptions = None
        self._statistics = None
        self._rowcount = -1
        self._idx = 0
        self._row_sets = []
        self._next_set_idx = 0
        self._query_id = ""

    def _row_set_from_response(
        self, response: Response
    ) -> Tuple[
        int,
        Optional[List[Column]],
        Optional[Statistics],
        Optional[List[List[RawColType]]],
    ]:
        """Fetch information about executed query from http response."""

        # Empty response is returned for insert query
        if response.headers.get("content-length", "") == "0":
            return (-1, None, None, None)
        try:
            # Skip parsing floats to properly parse them later
            query_data = response.json(parse_float=str)
            rowcount = int(query_data["rows"])
            descriptions: Optional[List[Column]] = [
                Column(d["name"], parse_type(d["type"]), None, None, None, None, None)
                for d in query_data["meta"]
            ]
            if not descriptions:
                descriptions = None
            statistics = Statistics(**query_data["statistics"])
            # Parse data during fetch
            rows = query_data["data"]
            return (rowcount, descriptions, statistics, rows)
        except (KeyError, ValueError) as err:
            raise DataError(f"Invalid query data format: {str(err)}")

    def _append_row_set(
        self,
        row_set: Tuple[
            int,
            Optional[List[Column]],
            Optional[Statistics],
            Optional[List[List[RawColType]]],
        ],
    ) -> None:
        """Store information about executed query."""
        self._row_sets.append(row_set)
        if self._next_set_idx == 0:
            # Populate values for first set
            self._pop_next_set()

    def _validate_server_side_async_settings(
        self,
        parameters: Sequence[Sequence[ParameterType]],
        queries: List[Union[SetParameter, str]],
        skip_parsing: bool = False,
        async_execution: Optional[bool] = False,
    ) -> None:
        if async_execution and self._set_parameters.get("use_standard_sql", "1") == "0":
            raise AsyncExecutionUnavailableError(
                "It is not possible to execute queries asynchronously if "
                "use_standard_sql=0."
            )
        if parameters and skip_parsing:
            logger.warning(
                "Query formatting parameters are provided but skip_parsing "
                "is specified. They will be ignored."
            )
        non_set_queries = 0
        for query in queries:
            if type(query) is not SetParameter:
                non_set_queries += 1
        if non_set_queries > 1 and async_execution:
            raise AsyncExecutionUnavailableError(
                "It is not possible to execute multi-statement "
                "queries asynchronously."
            )

    def _parse_row(self, row: List[RawColType]) -> List[ColType]:
        """Parse a single data row based on query column types."""
        assert len(row) == len(self.description)
        return [
            parse_value(col, self.description[i].type_code) for i, col in enumerate(row)
        ]

    def _get_next_range(self, size: int) -> Tuple[int, int]:
        """
        Return range of next rows of size (if possible),
        and update _idx to point to the end of this range
        """

        if self._rows is None:
            # No elements to take
            raise DataError("no rows to fetch")

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
        """
        Fetch the next set of rows of a query result;
        cursor.arraysize is default size.
        """
        size = size if size is not None else self.arraysize
        left, right = self._get_next_range(size)
        assert self._rows is not None
        rows = self._rows[left:right]
        return [self._parse_row(row) for row in rows]

    @check_not_closed
    @check_query_executed
    def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result."""
        left, right = self._get_next_range(self.rowcount)
        assert self._rows is not None
        rows = self._rows[left:right]
        return [self._parse_row(row) for row in rows]

    @check_not_closed
    def setinputsizes(self, sizes: List[int]) -> None:
        """Predefine memory areas for query parameters (does nothing)."""

    @check_not_closed
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns (does nothing)."""
