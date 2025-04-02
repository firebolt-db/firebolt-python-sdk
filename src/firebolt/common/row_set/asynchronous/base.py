from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Optional

from async_property import async_property  # type: ignore

from firebolt.common._types import ColType
from firebolt.common.row_set.types import AsyncByteStream, Column, Statistics


class BaseAsyncRowSet(ABC):
    """
    Base class for all async row sets.
    """

    @abstractmethod
    async def append_response_stream(self, stream: AsyncByteStream) -> None:
        ...

    @async_property
    @abstractmethod
    async def row_count(self) -> Optional[int]:
        ...

    @async_property
    @abstractmethod
    def statistics(self) -> Optional[Statistics]:
        ...

    @async_property
    @abstractmethod
    async def columns(self) -> List[Column]:
        ...

    @async_property
    @abstractmethod
    def nextset(self) -> bool:
        ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[List[ColType]]:
        ...

    @abstractmethod
    async def __anext__(self) -> List[ColType]:
        ...

    @abstractmethod
    async def aclose(self) -> None:
        ...
