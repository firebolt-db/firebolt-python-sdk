from functools import wraps
from typing import Any, Callable, Iterator, List, Optional

from httpx import HTTPError, Response

from firebolt.common._types import ColType
from firebolt.common.row_set.json_lines import DataRecord, JSONLinesRecord
from firebolt.common.row_set.streaming_common import StreamingRowSetCommonBase
from firebolt.common.row_set.synchronous.base import BaseSyncRowSet
from firebolt.common.row_set.types import Column, Statistics
from firebolt.utils.exception import OperationalError
from firebolt.utils.util import ExceptionGroup


def close_on_op_error(func: Callable) -> Callable:
    """
    Decorator to close the response on OperationalError.
    Args:
        func: Function to be decorated

    Returns:
        Callable: Decorated function

    """

    @wraps(func)
    def inner(self: "StreamingRowSet", *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except OperationalError:
            self.close()
            raise

    return inner


class StreamingRowSet(BaseSyncRowSet, StreamingRowSetCommonBase):
    """
    A row set that streams rows from a response.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lines_iter: Optional[Iterator[str]] = None

    def append_response(self, response: Response) -> None:
        """
        Append a response to the row set.

        Args:
            response: HTTP response to append

        Raises:
            OperationalError: If an error occurs while appending the response
        """
        self._responses.append(response)
        if len(self._responses) == 1:
            # First response, initialize the columns
            self._current_columns = self._fetch_columns()

    def append_empty_response(self) -> None:
        """
        Append an empty response to the row set.
        """
        self._responses.append(None)

    @close_on_op_error
    def _next_json_lines_record(self) -> Optional[JSONLinesRecord]:
        """
        Get the next JSON lines record from the current response stream.

        Returns:
            JSONLinesRecord or None if there are no more records

        Raises:
            OperationalError: If reading from the response stream fails
        """
        if self._current_response is None:
            return None
        if self._lines_iter is None:
            try:
                self._lines_iter = self._current_response.iter_lines()
            except HTTPError as err:
                raise OperationalError("Failed to read response stream.") from err

        next_line = next(self._lines_iter, None)
        return self._next_json_lines_record_from_line(next_line)

    @property
    def row_count(self) -> int:
        """
        Get the current row count.

        Returns:
            int: Number of rows processed, -1 if unknown
        """
        return self._current_row_count

    @close_on_op_error
    def _fetch_columns(self) -> List[Column]:
        """
        Fetch column metadata from the current response.

        Returns:
            List[Column]: List of column metadata objects

        Raises:
            OperationalError: If an error occurs while fetching columns
        """
        if self._current_response is None:
            return []
        record = self._next_json_lines_record()
        return self._fetch_columns_from_record(record)

    @property
    def columns(self) -> Optional[List[Column]]:
        """
        Get the column metadata for the current result set.

        Returns:
            List[Column]: List of column metadata objects
        """
        return self._current_columns

    @property
    def statistics(self) -> Optional[Statistics]:
        """
        Get query execution statistics for the current result set.

        Returns:
            Statistics or None: Statistics object if available, None otherwise
        """
        return self._current_statistics

    def nextset(self) -> bool:
        """
        Move to the next result set.

        Returns:
            bool: True if there is a next result set, False otherwise

        Raises:
            OperationalError: If the response stream cannot be closed or if an error
                occurs while fetching new columns
        """
        if self._current_row_set_idx + 1 < len(self._responses):
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except HTTPError as err:
                    self.close()
                    raise OperationalError("Failed to close response.") from err
            self._current_row_set_idx += 1
            self._reset()
            self._current_columns = self._fetch_columns()
            return True
        return False

    @close_on_op_error
    def _pop_data_record(self) -> Optional[DataRecord]:
        """
        Pop the next data record from the current response stream.

        Returns:
            DataRecord or None: The next data record
            or None if there are no more records

        Raises:
            OperationalError: If an error occurs while reading the record
        """
        record = self._next_json_lines_record()
        return self._pop_data_record_from_record(record)

    def __next__(self) -> List[ColType]:
        """
        Get the next row of data.

        Returns:
            List[ColType]: The next row of data

        Raises:
            StopIteration: If there are no more rows
            OperationalError: If an error occurs while reading the row
        """
        if self._current_response is None or self._response_consumed:
            raise StopIteration

        if self._current_record is None or self._current_record_row_idx >= len(
            self._current_record.data
        ):
            self._current_record = self._pop_data_record()
            self._current_record_row_idx = 0

        return self._get_next_data_row_from_current_record(StopIteration)

    def close(self) -> None:
        """
        Close the row set and all responses.

        This method ensures all HTTP responses are properly closed and resources
        are released.

        Raises:
            OperationalError: If an error occurs while closing the responses
        """
        errors: List[BaseException] = []
        for response in self._responses[self._current_row_set_idx :]:
            if response is not None and not response.is_closed:
                try:
                    response.close()
                except HTTPError as err:
                    errors.append(err)

        self._reset()
        self._responses = []

        # Propagate any errors that occurred during closing
        if errors:
            raise OperationalError("Failed to close row set.") from ExceptionGroup(
                "Errors during closing http streams.", errors
            )
