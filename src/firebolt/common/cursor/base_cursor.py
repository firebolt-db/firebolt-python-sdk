from __future__ import annotations

import logging
import re
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from httpx import URL, Response

from firebolt.common._types import RawColType, SetParameter
from firebolt.common.constants import (
    DISALLOWED_PARAMETER_LIST,
    IMMUTABLE_PARAMETER_LIST,
    JSON_LINES_OUTPUT_FORMAT,
    JSON_OUTPUT_FORMAT,
    USE_PARAMETER_LIST,
    CursorState,
)
from firebolt.common.cursor.decorators import check_not_closed
from firebolt.common.row_set.base import BaseRowSet
from firebolt.common.row_set.types import AsyncResponse, Column, Statistics
from firebolt.common.statement_formatter import StatementFormatter
from firebolt.utils.exception import ConfigurationError, FireboltError
from firebolt.utils.util import fix_url_schema

logger = logging.getLogger(__name__)


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


RowSet = Tuple[
    int,
    Optional[List[Column]],
    Optional[Statistics],
    Optional[List[List[RawColType]]],
]


class BaseCursor:
    __slots__ = (
        "connection",
        "parameters",
        "_arraysize",
        "_client",
        "_state",
        "_formatter",
        "_set_parameters",
        "_query_id",
        "_query_token",
        "_row_set",
        "engine_url",
    )

    default_arraysize = 1
    in_memory_row_set_type: Type = BaseRowSet
    streaming_row_set_type: Type = BaseRowSet

    def __init__(
        self, *args: Any, formatter: StatementFormatter, **kwargs: Any
    ) -> None:
        self._arraysize = self.default_arraysize
        # These fields initialized here for type annotations purpose
        self._formatter = formatter
        # User-defined set parameters
        self._set_parameters: Dict[str, Any] = dict()
        # Server-side parameters (user can't change them)
        self.parameters: Dict[str, str] = dict()
        self.engine_url = ""
        self._query_id = ""  # not used
        self._query_token = ""
        self._row_set: Optional[BaseRowSet] = None
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
        if not self._row_set:
            return None
        return self._row_set.columns

    @property  # type: ignore
    @check_not_closed
    def statistics(self) -> Optional[Statistics]:
        """Query execution statistics returned by the backend."""
        if not self._row_set:
            return None
        return self._row_set.statistics

    @property  # type: ignore
    @check_not_closed
    def rowcount(self) -> int:
        """The number of rows produced by last query."""
        if not self._row_set:
            return -1
        return self._row_set.row_count

    @property  # type: ignore
    @check_not_closed
    def query_id(self) -> str:
        """
        Deprecated: This property is not populated anymore and is left for
        backward compatibility.
        """
        # FIR-42243
        return self._query_id

    @property
    def async_query_token(self) -> str:
        """The query token of a query executed asynchronously."""
        if not self._query_token:
            raise FireboltError(
                "No async query was executed or query was not an async."
            )
        return self._query_token

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

    def flush_parameters(self) -> None:
        """Cleanup all previously set parameters"""
        self._set_parameters = dict()

    def _reset(self) -> None:
        """Clear all data stored from previous query."""
        self._state = CursorState.NONE
        self._row_set = None
        self._query_id = ""
        self._query_token = ""

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

    def _parse_async_response(self, response: Response) -> None:
        """Handle async response from the server."""
        async_response = AsyncResponse(**response.json())
        self._query_token = async_response.token

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

    @staticmethod
    def _get_output_format(is_streaming: bool) -> str:
        """
        Get the output format based on whether streaming is enabled or not.
        Args:
            is_streaming (bool): Flag indicating if streaming is enabled.

        Returns:
            str: The output format string.
        """
        if is_streaming:
            return JSON_LINES_OUTPUT_FORMAT
        return JSON_OUTPUT_FORMAT
