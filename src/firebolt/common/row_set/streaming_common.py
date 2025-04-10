import json
from typing import Iterator, List, Optional

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
        self._current_row_set_idx = 0

        # current row set
        self._lines_iter: Optional[Iterator[str]]
        self._rows_returned: int
        self._current_row_count: int
        self._current_statistics: Optional[Statistics]
        self._current_columns: Optional[List[Column]]
        self._response_consumed: bool

        # current json lines record
        self._current_record: Optional[DataRecord]
        self._current_record_row_idx: int

        self._reset()

    def _reset(self) -> None:
        """
        Reset the state of the streaming row set.
        """
        self._current_row_set_idx += 1
        self._current_row_count = -1
        self._current_statistics = None
        self._lines_iter = None
        self._current_record = None
        self._current_record_row_idx = -1
        self._response_consumed = False
        self._current_columns = None

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
        Generator that yields JSON lines from the current response stream.

        Args:
            next_line: The next line from the response stream.

        Returns:
            JSONLinesRecord: The parsed JSON lines record.
        Raises:
            OperationalError: If the JSON line is invalid or if it contains
            a record of invalid format.
        """
        if next_line is None:
            return None

        try:
            record = json.loads(next_line)
        except json.JSONDecodeError as err:
            raise OperationalError(
                f"Invalid JSON line response format: {next_line}"
            ) from err

        record = parse_json_lines_record(record)
        if isinstance(record, ErrorRecord):
            self._response_consumed = True
            self._current_statistics = record.statistics
            raise FireboltStructuredError(**record.errors[0])
        return record

    def _fetch_columns_from_record(
        self, record: Optional[JSONLinesRecord]
    ) -> List[Column]:
        """
        Fetch columns from the JSON lines record.

        Args:
            record: The JSON lines record to fetch columns from.
        Returns:
            List[Column]: The list of columns.
        Raises:
            OperationalError: If the JSON line is unexpectedly empty or
            if it's message type is unexpected.
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
        Pop the data record from the JSON lines record.
        Args:
            record: The JSON lines record to pop data from.
        Returns:
            Optional[DataRecord]: The data record.
        Raises:
            OperationalError: If the JSON line is unexpectedly empty or
            if it's message type is unexpected.
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

    def _get_next_data_row_from_current_record(self) -> List[ColType]:
        """
        Get the next data row from the current record.
        Returns:
            List[ColType]: The next data row.
        Raises:
            StopIteration: If there are no more rows to return.
        """
        if self._current_record is None:
            raise StopIteration

        data_row = self._parse_row(  # type: ignore
            self._current_record.data[self._current_record_row_idx]
        )
        self._current_record_row_idx += 1
        self._rows_returned += 1
        return data_row
