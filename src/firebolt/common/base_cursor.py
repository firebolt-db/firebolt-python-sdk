from __future__ import annotations

import logging
import re
from dataclasses import dataclass, fields
from enum import Enum
from functools import wraps
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from httpx import URL, Response

from firebolt.common._types import (
    ColType,
    Column,
    RawColType,
    SetParameter,
    parse_type,
    parse_value,
)
from firebolt.utils.exception import (
    ConfigurationError,
    CursorClosedError,
    DataError,
    QueryNotRunError,
)
from firebolt.utils.util import Timer, fix_url_schema

logger = logging.getLogger(__name__)


JSON_OUTPUT_FORMAT = "JSON_Compact"


class CursorState(Enum):
    NONE = 1
    ERROR = 2
    DONE = 3
    CLOSED = 4


# Parameters that should be set using USE instead of SET
USE_PARAMETER_LIST = ["database", "engine"]
# parameters that can only be set by the backend
DISALLOWED_PARAMETER_LIST = ["output_format"]
# parameters that are set by the backend and should not be set by the user
IMMUTABLE_PARAMETER_LIST = USE_PARAMETER_LIST + DISALLOWED_PARAMETER_LIST

UPDATE_ENDPOINT_HEADER = "Firebolt-Update-Endpoint"
UPDATE_PARAMETERS_HEADER = "Firebolt-Update-Parameters"
RESET_SESSION_HEADER = "Firebolt-Reset-Session"


def _parse_update_parameters(parameter_header: str) -> Dict[str, str]:
    """Parse update parameters and set them as attributes."""
    # parse key1=value1,key2=value2 comma separated string into dict
    param_dict = dict(item.split("=") for item in parameter_header.split(","))
    # strip whitespace from keys and values
    param_dict = {key.strip(): value.strip() for key, value in param_dict.items()}
    return param_dict


def _parse_update_endpoint(
    new_engine_endpoint_header: str,
) -> Tuple[str, Dict[str, str]]:
    endpoint = URL(fix_url_schema(new_engine_endpoint_header))
    return fix_url_schema(endpoint.host), dict(endpoint.params)


def _raise_if_internal_set_parameter(parameter: SetParameter) -> None:
    """
    Check if parameter is internal and raise an error if it is.
    """
    if parameter.name in USE_PARAMETER_LIST:
        raise ConfigurationError(
            "Could not set parameter. "
            f"Set parameter '{parameter.name}' is not allowed. "
            f"Try again with 'USE {str(parameter.name).upper()}' instead of SET"
        )
    if parameter.name in DISALLOWED_PARAMETER_LIST:
        raise ConfigurationError(
            "Could not set parameter. "
            f"Set parameter '{parameter.name}' is not allowed. "
            "Try again with a different parameter name."
        )


@dataclass
class Statistics:
    """
    Class for query execution statistics.
    """

    elapsed: float
    rows_read: int
    bytes_read: int
    time_before_execution: float
    time_to_execute: float
    scanned_bytes_cache: Optional[float] = None
    scanned_bytes_storage: Optional[float] = None

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            _type = eval(field.type)  # type: ignore

            # Unpack Optional
            if hasattr(_type, "__args__"):
                _type = _type.__args__[0]
            if value is not None and not isinstance(value, _type):
                # convert values to proper types
                setattr(self, field.name, _type(value))


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
        "parameters",
        "_arraysize",
        "_client",
        "_state",
        "_descriptions",
        "_statistics",
        "_rowcount",
        "_rows",
        "_idx",
        "_row_sets",
        "_next_set_idx",
        "_set_parameters",
        "_query_id",
        "engine_url",
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
        # User-defined set parameters
        self._set_parameters: Dict[str, Any] = dict()
        # Server-side parameters (user can't change them)
        self.parameters: Dict[str, str] = dict()
        self.engine_url = ""
        self._rowcount = -1
        self._idx = 0
        self._next_set_idx = 0
        self._query_id = ""
        self._reset()

    @property
    def database(self) -> Optional[str]:
        return self.parameters.get("database")

    @database.setter
    def database(self, database: str) -> None:
        self.parameters["database"] = database

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

    def _update_set_parameters(self, parameters: Dict[str, Any]) -> None:
        # Split parameters into immutable and user parameters
        immutable_parameters = {
            key: value
            for key, value in parameters.items()
            if key in IMMUTABLE_PARAMETER_LIST
        }
        user_parameters = {
            key: value
            for key, value in parameters.items()
            if key not in IMMUTABLE_PARAMETER_LIST
        }

        self.parameters.update(immutable_parameters)

        self._set_parameters.update(user_parameters)

    def _update_server_parameters(self, parameters: Dict[str, Any]) -> None:
        for key, value in parameters.items():
            self.parameters[key] = value

    @staticmethod
    def _log_query(query: Union[str, SetParameter]) -> None:
        # Our CREATE EXTERNAL TABLE queries currently require credentials,
        # so we will skip logging those queries.
        # https://docs.firebolt.io/sql-reference/commands/create-external-table.html
        if isinstance(query, SetParameter) or not re.search(
            "aws_key_id|credentials", query, flags=re.IGNORECASE
        ):
            logger.debug(f"Running query: {query}")

    @property
    def engine_name(self) -> str:
        """
        Get the name of the engine that we're using.

        Args:
            engine_url (str): URL of the engine
        """
        if self.parameters.get("engine"):
            return self.parameters["engine"]
        return URL(self.engine_url).host.split(".")[0].replace("-", "_")

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

    _performance_log_message = (
        "[PERFORMANCE] Parsing query output into native Python types"
    )

    @check_not_closed
    @check_query_executed
    def fetchone(self) -> Optional[List[ColType]]:
        """Fetch the next row of a query result set."""
        left, right = self._get_next_range(1)
        if left == right:
            # We are out of elements
            return None
        assert self._rows is not None
        with Timer(self._performance_log_message):
            result = self._parse_row(self._rows[left])
        return result

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
        with Timer(self._performance_log_message):
            result = [self._parse_row(row) for row in rows]
        return result

    @check_not_closed
    @check_query_executed
    def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result."""
        left, right = self._get_next_range(self.rowcount)
        assert self._rows is not None
        rows = self._rows[left:right]
        with Timer(self._performance_log_message):
            result = [self._parse_row(row) for row in rows]
        return result

    @check_not_closed
    def setinputsizes(self, sizes: List[int]) -> None:
        """Predefine memory areas for query parameters (does nothing)."""

    @check_not_closed
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns (does nothing)."""

    def close(self) -> None:
        """Terminate an ongoing query (if any) and mark connection as closed."""
        self._state = CursorState.CLOSED
        self.connection._remove_cursor(self)  # type:ignore

    def __del__(self) -> None:
        self.close()

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()
