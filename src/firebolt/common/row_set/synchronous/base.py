from abc import ABC, abstractmethod
from typing import Iterator, List, Optional

from firebolt.common._types import ColType
from firebolt.common.row_set.types import ByteStream, Column, Statistics


class BaseRowSet(ABC):
    """
    Base class for all sync row sets.
    """

    @abstractmethod
    def append_response_stream(self, stream: ByteStream) -> None:
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
    def __iter__(self) -> Iterator[List[ColType]]:
        ...

    @abstractmethod
    def __next__(self) -> List[ColType]:
        ...

    @abstractmethod
    def close(self) -> None:
        ...
