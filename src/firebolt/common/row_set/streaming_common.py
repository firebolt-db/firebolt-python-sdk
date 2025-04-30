import json
from typing import Any, AsyncIterator, Iterator, List, Optional, Type, Union

from httpx import Response

from firebolt.common._types import ColType, parse_type
from firebolt.common.row_set.json_lines import (
    DataRecord,
    ErrorRecord,
    JSONLinesRecord,
    StartRecord,
    SuccessRecord,
    parse_json_lines_record,
)
from firebolt.common.row_set.types import Column, Statistics
from firebolt.utils.exception import (
    DataError,
    FireboltStructuredError,
    OperationalError,
)


class StreamingRowSetCommonBase:
    """
    A mixin class that provides common functionality for streaming row sets.
    """

    def __init__(self) -> None:
        self._responses: List[Optional[Response]] = []
        self._current_row_set_idx: int = 0

        # current row set
        self._rows_returned: int
        self._current_row_count: int
        self._current_statistics: Optional[Statistics]
        self._current_columns: Optional[List[Column]] = None
        self._response_consumed: bool

        # current json lines record
        self._current_record: Optional[DataRecord]
        self._current_record_row_idx: int

        self._reset()

    def _reset(self) -> None:
        """
        Reset the state of the streaming row set.

        Resets internal counters, iterators, and cached data for the next row set.
        Note: Does not reset _current_row_set_idx to allow for multiple row sets.
        """
        self._current_row_count = -1
        self._current_statistics = None
        self._lines_ite: Optional[Union[AsyncIterator[str], Iterator[str]]] = None
        self._current_record = None
        self._current_record_row_idx = -1
        self._response_consumed = False
        self._current_columns = None
        self._rows_returned = 0

    @property
    def _current_response(self) -> Optional[Response]:
        """
        Get the current response.

        Returns:
            Optional[Response]: The current response.

        Raises:
            DataError: If no results are available.
        """
        if self._current_row_set_idx >= len(self._responses):
            raise DataError("No results available.")
        return self._responses[self._current_row_set_idx]

    def _next_json_lines_record_from_line(
        self, next_line: Optional[str]
    ) -> Optional[JSONLinesRecord]:
        """
        Parse a JSON line into a JSONLinesRecord.

        Args:
            next_line: The next line from the response stream.

        Returns:
            JSONLinesRecord: The parsed JSON lines record, or None if line is None.

        Raises:
            OperationalError: If the JSON line is invalid or if it contains
                a record of invalid format.
            FireboltStructuredError: If the record contains error information.
        """
        if next_line is None:
            return None

        try:
            # Skip parsing floats to properly parse them later
            record = json.loads(next_line, parse_float=str)
        except json.JSONDecodeError as err:
            raise OperationalError(
                f"Invalid JSON line response format: {next_line}"
            ) from err

        record = parse_json_lines_record(record)
        if isinstance(record, ErrorRecord):
            self._response_consumed = True
            self._current_statistics = record.statistics
            self._handle_error_record(record)
        return record

    def _handle_error_record(self, record: ErrorRecord) -> None:
        """
        Handle an error record by raising the appropriate exception.

        Args:
            record: The error record to handle.

        Raises:
            FireboltStructuredError: With details from the error record.
        """
        raise FireboltStructuredError({"errors": record.errors})

    def _fetch_columns_from_record(
        self, record: Optional[JSONLinesRecord]
    ) -> List[Column]:
        """
        Extract column definitions from a JSON lines record.

        Args:
            record: The JSON lines record to fetch columns from.

        Returns:
            List[Column]: The list of columns.

        Raises:
            OperationalError: If the JSON line is unexpectedly empty or
                if its message type is unexpected.
        """
        if record is None:
            self._response_consumed = True
            raise OperationalError(
                "Unexpected end of response stream while reading columns."
            )
        if not isinstance(record, StartRecord):
            self._response_consumed = True
            raise OperationalError(
                f"Unexpected json line message type {record.message_type.value}, "
                "expected START"
            )

        return [
            Column(col.name, parse_type(col.type), None, None, None, None, None)
            for col in record.result_columns
        ]

    def _pop_data_record_from_record(
        self, record: Optional[JSONLinesRecord]
    ) -> Optional[DataRecord]:
        """
        Extract a data record from a JSON lines record.

        Args:
            record: The JSON lines record to pop data from.

        Returns:
            Optional[DataRecord]: The data record or None if no more data is available.

        Raises:
            OperationalError: If the JSON line is unexpectedly empty or
                if its message type is unexpected.
        """
        if record is None:
            if not self._response_consumed:
                self._response_consumed = True
                raise OperationalError(
                    "Unexpected end of response stream while reading data."
                )
            return None

        if isinstance(record, SuccessRecord):
            # we're done reading, set the row count and statistics
            self._current_row_count = self._rows_returned
            self._current_statistics = record.statistics
            self._response_consumed = True
            return None
        if not isinstance(record, DataRecord):
            raise OperationalError(
                f"Unexpected json line message type {record.message_type.value}, "
                "expected DATA"
            )
        return record

    def _parse_row(self, row_data: Any) -> List[ColType]:
        """
        Parse a row of data from raw format to typed values.

        This is an abstract method that must be implemented by subclasses.

        Args:
            row_data: Raw row data to be parsed

        Returns:
            List[ColType]: Parsed row data with proper types

        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement _parse_row")

    def _get_next_data_row_from_current_record(
        self, stop_iteration_err_cls: Type[Union[StopIteration, StopAsyncIteration]]
    ) -> List[ColType]:
        """
        Extract the next data row from the current record.

        Returns:
            List[ColType]: The next data row with parsed column values.

        Raises:
            StopIteration: If there are no more rows to return.
        """
        if self._current_record is None:
            raise stop_iteration_err_cls

        data_row = self._parse_row(
            self._current_record.data[self._current_record_row_idx]
        )
        self._current_record_row_idx += 1
        self._rows_returned += 1
        return data_row
