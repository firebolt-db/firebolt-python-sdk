from __future__ import annotations

from dataclasses import dataclass, fields
from typing import AsyncIterator, Iterator, List, Optional, Protocol, Union

from firebolt.common._types import ExtendedType, RawColType


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


@dataclass
class RowsResponse:
    """
    Class for query execution response.
    """

    row_count: int
    columns: List[Column]
    statistics: Optional[Statistics]
    rows: List[List[RawColType]]


@dataclass
class Column:
    name: str
    type_code: Union[type, ExtendedType]
    display_size: Optional[int] = None
    internal_size: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    null_ok: Optional[bool] = None


class ByteStream(Protocol):
    def __iter__(self) -> Iterator[bytes]:
        ...

    def close(self) -> None:
        ...


class AsyncByteStream(Protocol):
    def __aiter__(self) -> AsyncIterator[bytes]:
        ...

    async def aclose(self) -> None:
        ...
