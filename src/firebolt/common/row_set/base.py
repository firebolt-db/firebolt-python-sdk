from abc import ABC, abstractmethod
from typing import List, Optional

from firebolt.common.row_set.types import Column, Statistics


class BaseRowSet(ABC):
    """
    Base class for all async row sets.
    """

    @property
    @abstractmethod
    def row_count(self) -> int:
        ...

    @property
    @abstractmethod
    def statistics(self) -> Optional[Statistics]:
        ...

    @property
    @abstractmethod
    def columns(self) -> List[Column]:
        ...

    @abstractmethod
    def nextset(self) -> bool:
        ...

    @abstractmethod
    def append_empty_response(self) -> None:
        ...
