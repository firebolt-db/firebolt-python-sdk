import json
import re
from typing import Any, Dict, Iterator, List, Optional

from httpx import Response

from firebolt.common._types import ColType, parse_type
from firebolt.common.row_set.synchronous.base import BaseSyncRowSet
from firebolt.common.row_set.types import Column, RowsResponse, Statistics
from firebolt.utils.exception import DataError, FireboltStructuredError

# Whitespace as JSON defines it (RFC 8259), which is narrower than str.isspace()
_JSON_WHITESPACE = re.compile(r"[ \t\n\r]*")


class InMemoryRowSet(BaseSyncRowSet):
    """
    A row set that holds all rows in memory.
    """

    def __init__(self) -> None:
        self._row_sets: List[RowsResponse] = []
        self._current_row_set_idx = 0
        self._current_row = -1

    def append_empty_response(self) -> None:
        """
        Create an InMemoryRowSet from an empty response.
        """
        self._row_sets.append(RowsResponse(-1, [], None, []))

    def append_response(self, response: Response) -> None:
        """
        Create an InMemoryRowSet from a response.
        """
        try:
            self.append_response_stream(response.iter_bytes())
        finally:
            response.close()

    def append_response_stream(self, stream: Iterator[bytes]) -> None:
        """
        Create an InMemoryRowSet from a response stream.
        """
        content = b"".join(stream).decode("utf-8")
        if len(content) == 0:
            self.append_empty_response()
            return
        try:
            row_sets = [
                self._parse_query_data(query_data)
                for query_data in self._iter_documents(content)
            ]
            if not row_sets:
                raise ValueError("no result set found in response")
        except (KeyError, TypeError, ValueError) as err:
            raise DataError(f"Invalid query data format: {str(err)}")
        # Only a fully parsed response contributes result sets
        self._row_sets.extend(row_sets)

    @staticmethod
    def _iter_documents(content: str) -> Iterator[Dict[str, Any]]:
        """
        Yield every JSON document of a response, one per result set.
        """
        # Skip parsing floats to properly parse them later
        decoder = json.JSONDecoder(parse_float=str)
        position = 0
        while True:
            whitespace = _JSON_WHITESPACE.match(content, position)
            if whitespace is not None:
                position = whitespace.end()
            if position == len(content):
                return
            try:
                document, position = decoder.raw_decode(content, position)
            except json.JSONDecodeError as err:
                if position == 0:
                    raise
                # Keep the text the engine wrote after a complete document, which
                # is where it reports an error of a later statement
                raise ValueError(
                    f"{err}; unexpected content after a result set: "
                    f"{content[position:position + 500]!r}"
                ) from err
            yield document

    @staticmethod
    def _parse_query_data(query_data: Any) -> RowsResponse:
        """
        Build a result set from a parsed JSON document.
        """
        if not isinstance(query_data, dict):
            raise ValueError(f"expected a JSON object, got {type(query_data).__name__}")
        if "errors" in query_data and len(query_data["errors"]) > 0:
            raise FireboltStructuredError(query_data)

        columns = [
            Column(d["name"], parse_type(d["type"]), None, None, None, None, None)
            for d in query_data["meta"]
        ]
        # Extract rows
        rows = query_data["data"]
        row_count = len(rows)
        statistics = Statistics(**query_data.get("statistics", {}))
        return RowsResponse(row_count, columns, statistics, rows)

    @property
    def _row_set(self) -> RowsResponse:
        if self._current_row_set_idx >= len(self._row_sets):
            raise DataError("No results available.")
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

    def __next__(self) -> List[ColType]:
        if self._row_set.row_count == -1:
            raise DataError("no rows to fetch")
        self._current_row += 1
        if self._current_row >= self._row_set.row_count:
            raise StopIteration
        return self._parse_row(self._row_set.rows[self._current_row])

    def close(self) -> None:
        # No-op for in-memory row set
        pass
