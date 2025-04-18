import io
from typing import List, Optional

from httpx import Response

from firebolt.common._types import ColType
from firebolt.common.row_set.asynchronous.base import BaseAsyncRowSet
from firebolt.common.row_set.synchronous.in_memory import InMemoryRowSet
from firebolt.common.row_set.types import Column, Statistics


class InMemoryAsyncRowSet(BaseAsyncRowSet):
    """A row set that holds all rows in memory.

    This async implementation relies on the synchronous InMemoryRowSet class for
    core functionality while providing async-compatible interfaces.
    """

    def __init__(self) -> None:
        """Initialize an asynchronous in-memory row set."""
        self._sync_row_set = InMemoryRowSet()

    def append_empty_response(self) -> None:
        """Append an empty response to the row set."""
        self._sync_row_set.append_empty_response()

    async def append_response(self, response: Response) -> None:
        """Append response data to the row set.

        Args:
            response: HTTP response to append

        Note:
            The response will be fully buffered in memory.
        """
        try:
            sync_stream = io.BytesIO(
                b"".join([b async for b in response.aiter_bytes()])
            )
            self._sync_row_set.append_response_stream(sync_stream)
        finally:
            await response.aclose()

    @property
    def row_count(self) -> int:
        """Get the number of rows in the current result set.

        Returns:
            int: The number of rows, or -1 if unknown
        """
        return self._sync_row_set.row_count

    @property
    def columns(self) -> List[Column]:
        """Get the column metadata for the current result set.

        Returns:
            List[Column]: List of column metadata objects
        """
        return self._sync_row_set.columns

    @property
    def statistics(self) -> Optional[Statistics]:
        """Get query execution statistics for the current result set.

        Returns:
            Statistics or None: Statistics object if available, None otherwise
        """
        return self._sync_row_set.statistics

    async def nextset(self) -> bool:
        """Move to the next result set.

        Returns:
            bool: True if there is a next result set, False otherwise
        """
        return self._sync_row_set.nextset()

    async def __anext__(self) -> List[ColType]:
        """Get the next row of data asynchronously.

        Returns:
            List[ColType]: The next row of data

        Raises:
            StopAsyncIteration: If there are no more rows
        """
        try:
            return next(self._sync_row_set)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self) -> None:
        """Close the row set asynchronously.

        This releases any resources held by the row set.
        """
        return self._sync_row_set.close()
