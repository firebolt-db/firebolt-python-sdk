from abc import ABC, abstractmethod
from typing import List, Optional

from httpx import SyncByteStream

from firebolt.common._types import ColType
from firebolt.common.row_set.types import Column, Statistics


class BaseRowSet(ABC):
    """
    Base class for all sync row sets.
    """

    @classmethod
    @abstractmethod
    def from_response_stream(cls, stream: SyncByteStream) -> "BaseRowSet":
        ...

    @property
    @abstractmethod
    def row_count(self) -> Optional[int]:
        # This is optional because for streaming it will not be available
        # until all rows are read
        ...

    @property
    @abstractmethod
    def statistics(self) -> Optional[Statistics]:
        # This is optional because for streaming it will not be available
        # until all rows are read
        ...

    @property
    @abstractmethod
    def columns(self) -> List[Column]:
        ...

    @abstractmethod
    def nextset(self) -> bool:
        ...

    @abstractmethod
    def __iter__(self) -> "BaseRowSet":
        ...

    @abstractmethod
    def __next__(self) -> List[ColType]:
        ...

    @abstractmethod
    def close(self) -> None:
        ...
