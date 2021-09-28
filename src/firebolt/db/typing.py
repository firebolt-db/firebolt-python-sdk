from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from functools import cached_property
from typing import Union, get_args

from firebolt.common.exception import DataError, NotSupportedError

_NoneType = type(None)
ColType = Union[int, float, str, datetime, date, bool, list, _NoneType]
RawColType = Union[int, float, str, bool, list]

# These definitions are required by PEP-249
Date = date


def DateFromTicks(t: int) -> date:
    return datetime.fromtimestamp(t).date()


def Time(hour: int, minute: int, second: int) -> None:
    raise NotSupportedError("time is not supported by Firebolt")


def TimeFromTicks(t: int) -> None:
    raise NotSupportedError("time is not supported by Firebolt")


Timestamp = datetime
TimestampFromTicks = datetime.fromtimestamp


def Binary(value: str) -> str:
    return value


STRING = BINARY = str
NUMBER = int
DATETIME = datetime
ROWID = int


class ARRAY:
    _prefix = "Array("

    def __init__(self, subtype: Union[type, ARRAY]):
        assert (subtype in get_args(ColType) and subtype is not list) or isinstance(
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
        types = {
            self.UInt8: int,
            self.UInt16: int,
            self.Int32: int,
            self.UInt32: int,
            self.Int64: int,
            self.UInt64: int,
            self.Float32: float,
            self.Float64: float,
            self.String: str,
            self.Date: date,
            self.DateTime: datetime,
            # For simplicity, this could happen only during 'select null' query
            self.Nothing: str,
        }
        return types[self.name]


def parse_type(raw_type: str) -> Union[type, ARRAY]:
    """Parse typename, provided by query metadata into python type"""
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


DATE_FORMAT: str = "%Y-%m-%d"
DATETIME_FORMAT: str = f"{DATE_FORMAT} %H:%M:%S"


def parse_value(
    value: RawColType,
    ctype: Union[type, ARRAY],
) -> ColType:
    if value is None:
        return None
    if ctype in (int, str, float):
        assert isinstance(ctype, type)
        return ctype(value)
    if ctype is date:
        if not isinstance(value, str):
            raise DataError(f"Invalid date value {value}: str expected")
        assert isinstance(value, str)
        return datetime.strptime(value, DATE_FORMAT).date()
    if ctype is datetime:
        if not isinstance(value, str):
            raise DataError(f"Invalid datetime value {value}: str expected")
        return datetime.strptime(value, DATETIME_FORMAT)
    if isinstance(ctype, ARRAY):
        assert isinstance(value, list)
        return [parse_value(it, ctype.subtype) for it in value]
    raise DataError(f"Unsupported data type returned: {ctype.__name__}")


def format_value(value: ColType) -> str:
    # TODO: FIR-7793
    pass
