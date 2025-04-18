from abc import ABC, abstractmethod
from typing import Iterator, List

from httpx import Response

from firebolt.common._types import ColType
from firebolt.common.row_set.base import BaseRowSet


class BaseSyncRowSet(BaseRowSet, ABC):
    """
    Base class for all sync row sets.
    """

    @abstractmethod
    def append_response(self, response: Response) -> None:
        ...

    def __iter__(self) -> Iterator[List[ColType]]:
        return self

    @abstractmethod
    def __next__(self) -> List[ColType]:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def nextset(self) -> bool:
        ...
