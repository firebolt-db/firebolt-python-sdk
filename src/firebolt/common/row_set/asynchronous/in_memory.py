import io
from typing import AsyncIterator, List, Optional

from firebolt.common._types import ColType
from firebolt.common.row_set.asynchronous.base import BaseAsyncRowSet
from firebolt.common.row_set.synchronous.in_memory import InMemoryRowSet
from firebolt.common.row_set.types import AsyncByteStream, Column, Statistics


class InMemoryAsyncRowSet(BaseAsyncRowSet):
    """
    A row set that holds all rows in memory.
    """

    def __init__(self) -> None:
        self._sync_row_set = InMemoryRowSet()

    async def append_response_stream(self, stream: AsyncByteStream) -> None:
        sync_stream = io.BytesIO(b"".join([b async for b in stream]))
        self._sync_row_set.append_response_stream(sync_stream)

    @property
    async def row_count(self) -> int:
        return self._sync_row_set.row_count

    @property
    async def columns(self) -> List[Column]:
        return self._sync_row_set.columns

    @property
    def statistics(self) -> Optional[Statistics]:
        return self._sync_row_set.statistics

    def nextset(self) -> bool:
        return self._sync_row_set.nextset()

    def __aiter__(self) -> AsyncIterator[List[ColType]]:
        return self

    async def __anext__(self) -> List[ColType]:
        return next(self._sync_row_set)

    async def aclose(self) -> None:
        return self._sync_row_set.close()
