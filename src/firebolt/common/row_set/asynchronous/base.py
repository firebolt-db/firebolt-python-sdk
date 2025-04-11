from abc import ABC, abstractmethod
from typing import AsyncIterator, List

from httpx import Response

from firebolt.common._types import ColType
from firebolt.common.row_set.base import BaseRowSet


class BaseAsyncRowSet(BaseRowSet, ABC):
    """
    Base class for all async row sets.
    """

    @abstractmethod
    async def append_response(self, response: Response) -> None:
        ...

    def __aiter__(self) -> AsyncIterator[List[ColType]]:
        return self

    @abstractmethod
    async def __anext__(self) -> List[ColType]:
        ...

    @abstractmethod
    async def aclose(self) -> None:
        ...

    @abstractmethod
    async def nextset(self) -> bool:
        ...
