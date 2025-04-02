from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass, fields
from typing import Optional


@dataclass
class AsyncResponse:
    token: str
    message: str
    monitorSql: str


@dataclass
class Statistics:
    """
    Class for query execution statistics.
    """

    elapsed: float
    rows_read: int
    bytes_read: int
    time_before_execution: float
    time_to_execute: float
    scanned_bytes_cache: Optional[float] = None
    scanned_bytes_storage: Optional[float] = None

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            _type = eval(field.type)  # type: ignore

            # Unpack Optional
            if hasattr(_type, "__args__"):
                _type = _type.__args__[0]
            if value is not None and not isinstance(value, _type):
                # convert values to proper types
                setattr(self, field.name, _type(value))


Column = namedtuple(
    "Column",
    (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    ),
)
