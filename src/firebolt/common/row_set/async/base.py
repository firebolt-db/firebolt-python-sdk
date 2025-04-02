from abc import ABC, abstractmethod
from typing import List, Optional

from async_property import async_property  # type: ignore
from httpx import AsyncByteStream

from firebolt.common._types import ColType
from firebolt.common.row_set.types import Column, Statistics


class BaseAsyncRowSet(ABC):
    """
    Base class for all async row sets.
    """

    @classmethod
    @abstractmethod
    async def from_response_stream(cls, stream: AsyncByteStream) -> "BaseAsyncRowSet":
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
    async def __aiter__(self) -> "BaseAsyncRowSet":
        ...

    @abstractmethod
    async def __anext__(self) -> List[ColType]:
        ...

    @abstractmethod
    async def aclose(self) -> None:
        ...
