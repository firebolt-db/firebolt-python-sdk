import io
from typing import List, Optional

from httpx import Response

from firebolt.common._types import ColType
from firebolt.common.row_set.asynchronous.base import BaseAsyncRowSet
from firebolt.common.row_set.synchronous.in_memory import InMemoryRowSet
from firebolt.common.row_set.types import Column, Statistics


class InMemoryAsyncRowSet(BaseAsyncRowSet):
    """
    A row set that holds all rows in memory.
    """

    def __init__(self) -> None:
        self._sync_row_set = InMemoryRowSet()

    def append_empty_response(self) -> None:
        self._sync_row_set.append_empty_response()

    async def append_response(self, response: Response) -> None:
        sync_stream = io.BytesIO(b"".join([b async for b in response.aiter_bytes()]))
        self._sync_row_set.append_response_stream(sync_stream)
        await response.aclose()

    @property
    def row_count(self) -> int:
        return self._sync_row_set.row_count

    @property
    def columns(self) -> List[Column]:
        return self._sync_row_set.columns

    @property
    def statistics(self) -> Optional[Statistics]:
        return self._sync_row_set.statistics

    def nextset(self) -> bool:
        return self._sync_row_set.nextset()

    async def __anext__(self) -> List[ColType]:
        try:
            return next(self._sync_row_set)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self) -> None:
        return self._sync_row_set.close()
