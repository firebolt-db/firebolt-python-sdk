from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime
from enum import Enum
from typing import Union

try:
    from ciso8601 import parse_datetime  # type: ignore
except ImportError:
    parse_datetime = datetime.fromisoformat  # type: ignore


from firebolt.common.exception import DataError, NotSupportedError
from firebolt.common.util import cached_property

_NoneType = type(None)
_col_types = (int, float, str, datetime, date, bool, list, _NoneType)
# duplicating this since 3.7 can't unpack Union
ColType = Union[int, float, str, datetime, date, bool, list, _NoneType]
RawColType = Union[int, float, str, bool, list, _NoneType]

# These definitions are required by PEP-249
Date = date


def DateFromTicks(t: int) -> date:
    """Convert ticks to date for firebolt db."""
    return datetime.fromtimestamp(t).date()


def Time(hour: int, minute: int, second: int) -> None:
    """Unsupported: construct time for firebolt db."""
    raise NotSupportedError("time is not supported by Firebolt")


def TimeFromTicks(t: int) -> None:
    """Unsupported: convert ticks to time for firebolt db."""
    raise NotSupportedError("time is not supported by Firebolt")


Timestamp = datetime
TimestampFromTicks = datetime.fromtimestamp


def Binary(value: str) -> str:
    """Convert string to binary for firebolt db, does nothing."""
    return value


STRING = BINARY = str
NUMBER = int
DATETIME = datetime
ROWID = int

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


class ARRAY:
    """Class for holding information about array column type in firebolt db."""

    _prefix = "Array("

    def __init__(self, subtype: Union[type, ARRAY]):
        assert (subtype in _col_types and subtype is not list) or isinstance(
            subtype, ARRAY
        ), f"Invalid array subtype: {str(subtype)}"
        self.subtype = subtype

    def __str__(self) -> str:
        return f"Array({str(self.subtype)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ARRAY):
            return NotImplemented
        return other.subtype == self.subtype


NULLABLE_PREFIX = "Nullable("


class _InternalType(Enum):
    """Enum of all internal firebolt types except for array."""

    # INT, INTEGER
    UInt8 = "UInt8"
    UInt16 = "UInt16"
    Int32 = "Int32"
    UInt32 = "UInt32"

    # BIGINT, LONG
    Int64 = "Int64"
    UInt64 = "UInt64"

    # FLOAT
    Float32 = "Float32"

    # DOUBLE, DOUBLE PRECISION
    Float64 = "Float64"

    # VARCHAR, TEXT, STRING
    String = "String"

    # DATE
    Date = "Date"

    # DATETIME, TIMESTAMP
    DateTime = "DateTime"

    # Nullable(Nothing)
    Nothing = "Nothing"

    @cached_property
    def python_type(self) -> type:
        """Convert internal type to python type."""
        types = {
            _InternalType.UInt8: int,
            _InternalType.UInt16: int,
            _InternalType.Int32: int,
            _InternalType.UInt32: int,
            _InternalType.Int64: int,
            _InternalType.UInt64: int,
            _InternalType.Float32: float,
            _InternalType.Float64: float,
            _InternalType.String: str,
            _InternalType.Date: date,
            _InternalType.DateTime: datetime,
            # For simplicity, this could happen only during 'select null' query
            _InternalType.Nothing: str,
        }
        return types[self]


def parse_type(raw_type: str) -> Union[type, ARRAY]:
    """Parse typename, provided by query metadata into python type."""
    if not isinstance(raw_type, str):
        raise DataError(f"Invalid typename {str(raw_type)}: str expected")
    # Handle arrays
    if raw_type.startswith(ARRAY._prefix) and raw_type.endswith(")"):
        return ARRAY(parse_type(raw_type[len(ARRAY._prefix) : -1]))
    # Handle nullable
    if raw_type.startswith(NULLABLE_PREFIX) and raw_type.endswith(")"):
        return parse_type(raw_type[len(NULLABLE_PREFIX) : -1])

    try:
        return _InternalType(raw_type).python_type
    except ValueError:
        # Treat unknown types as strings. Better that error since user still has
        # a way to work with it
        return str


def parse_value(
    value: RawColType,
    ctype: Union[type, ARRAY],
) -> ColType:
    """Provided raw value and python type, parses first into python value."""
    if value is None:
        return None
    if ctype in (int, str, float):
        assert isinstance(ctype, type)
        return ctype(value)
    if ctype is date:
        if not isinstance(value, str):
            raise DataError(f"Invalid date value {value}: str expected")
        assert isinstance(value, str)
        return parse_datetime(value).date()
    if ctype is datetime:
        if not isinstance(value, str):
            raise DataError(f"Invalid datetime value {value}: str expected")
        return parse_datetime(value)
    if isinstance(ctype, ARRAY):
        assert isinstance(value, list)
        return [parse_value(it, ctype.subtype) for it in value]
    raise DataError(f"Unsupported data type returned: {ctype.__name__}")


def format_value(value: ColType) -> str:
    # TODO: FIR-7793
    pass
