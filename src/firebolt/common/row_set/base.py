from abc import ABC, abstractmethod
from typing import List, Optional

from firebolt.common._types import ColType, RawColType, parse_value
from firebolt.common.row_set.types import Column, Statistics
from firebolt.utils.exception import OperationalError


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
    def columns(self) -> Optional[List[Column]]:
        ...

    @abstractmethod
    def append_empty_response(self) -> None:
        ...

    def _parse_row(self, row: List[RawColType]) -> List[ColType]:
        if self.columns is None:
            raise OperationalError("No columns definitions available yet.")
        assert len(row) == len(self.columns)
        return [
            parse_value(col, self.columns[i].type_code) for i, col in enumerate(row)
        ]
