from typing import List, Optional

from httpx import Response

from firebolt.common._types import ColType
from firebolt.common.row_set.json_lines import DataRecord, JSONLinesRecord
from firebolt.common.row_set.streaming_common import StreamingRowSetCommonBase
from firebolt.common.row_set.synchronous.base import BaseSyncRowSet
from firebolt.common.row_set.types import Column, Statistics


class StreamingRowSet(BaseSyncRowSet, StreamingRowSetCommonBase):
    """
    A row set that streams rows from a response.
    """

    def append_response(self, response: Response) -> None:
        """
        Append a response to the row set.
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

    def _next_json_lines_record(self) -> Optional[JSONLinesRecord]:
        """
        Generator that yields JSON lines from the current response stream.
        """
        if self._current_response is None:
            return None
        if self._lines_iter is None:
            self._lines_iter = self._current_response.iter_lines()

        next_line = next(self._lines_iter, None)
        return self._next_json_lines_record_from_line(next_line)

    @property
    def row_count(self) -> int:
        return self._current_row_count

    def _fetch_columns(self) -> List[Column]:
        if self._current_response is None:
            return []
        record = self._next_json_lines_record()
        return self._fetch_columns_from_record(record)

    @property
    def columns(self) -> List[Column]:
        if self._current_columns is None:
            self._current_columns = self._fetch_columns()
        return self._current_columns

    @property
    def statistics(self) -> Optional[Statistics]:
        return self._current_statistics

    def nextset(self) -> bool:
        """
        Move to the next result set.
        """
        if self._current_row_set_idx + 1 < len(self._responses):
            if self._current_response is not None:
                self._current_response.close()
            self._reset()
            self._current_columns = self._fetch_columns()
            return True
        return False

    def _pop_data_record(self) -> Optional[DataRecord]:
        """
        Pop the next data record from the current response stream.
        """
        record = self._next_json_lines_record()
        return self._pop_data_record_from_record(record)

    def __next__(self) -> List[ColType]:
        if self._current_response is None or self._response_consumed:
            raise StopIteration

        self._current_record_row_idx += 1
        if self._current_record is None or self._current_record_row_idx >= len(
            self._current_record.data
        ):
            self._current_record = self._pop_data_record()
            self._current_record_row_idx = -1

        return self._get_next_data_row_from_current_record()

    def close(self) -> None:
        """
        Close the row set and all responses.
        """
        for response in self._responses[self._current_row_set_idx :]:
            if response is not None and not response.is_closed:
                response.close()
        self._reset()
        self._responses = []
