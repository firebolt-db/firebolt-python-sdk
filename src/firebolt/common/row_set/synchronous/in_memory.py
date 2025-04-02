import json
from typing import Iterator, List, Optional

from firebolt.common._types import ColType, RawColType, parse_type, parse_value
from firebolt.common.row_set.synchronous.base import BaseRowSet
from firebolt.common.row_set.types import (
    ByteStream,
    Column,
    RowsResponse,
    Statistics,
)
from firebolt.utils.exception import DataError


class InMemoryRowSet(BaseRowSet):
    """
    A row set that holds all rows in memory.
    """

    def __init__(self) -> None:
        self._row_sets: List[RowsResponse] = []
        self._current_row_set_idx = 0
        self._current_row = -1

    def append_response_stream(self, stream: ByteStream) -> None:
        """
        Create an InMemoryRowSet from a response stream.
        """
        try:
            content = b"".join(stream)
            query_data = json.loads(content)
            columns = [
                Column(d["name"], parse_type(d["type"]), None, None, None, None, None)
                for d in query_data["meta"]
            ]
            # Extract rows
            rows = query_data["data"]
            row_count = len(rows)
            statistics = query_data.get("statistics")
            self._row_sets.append(RowsResponse(row_count, columns, statistics, rows))
        except (KeyError, ValueError) as err:
            raise DataError(f"Invalid query data format: {str(err)}")

    @property
    def _row_set(self) -> RowsResponse:
        return self._row_sets[self._current_row_set_idx]

    @property
    def row_count(self) -> int:
        return self._row_set.row_count

    @property
    def columns(self) -> List[Column]:
        return self._row_set.columns

    @property
    def statistics(self) -> Optional[Statistics]:
        return self._row_set.statistics

    def nextset(self) -> bool:
        if self._current_row_set_idx + 1 < len(self._row_sets):
            self._current_row_set_idx += 1
            self._current_row = -1
            return True
        return False

    def _parse_row(self, row: List[RawColType]) -> List[ColType]:
        assert len(row) == len(self.columns)
        return [
            parse_value(col, self.columns[i].type_code) for i, col in enumerate(row)
        ]

    def __iter__(self) -> Iterator[List[ColType]]:
        return self

    def __next__(self) -> List[ColType]:
        self._current_row += 1
        if self._current_row >= self._row_set.row_count:
            raise StopIteration
        return self._parse_row(self._row_set.rows[self._current_row])

    def close(self) -> None:
        pass
