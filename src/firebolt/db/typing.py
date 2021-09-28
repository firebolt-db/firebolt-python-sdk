from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from functools import cached_property
from typing import Union

from firebolt.common.exception import DataError

ColType = Union[int, float, str, datetime, date, bool, list]

# These definitions are required by PEP-249
Date = date


def DateFromTicks(t: int) -> date:
    return datetime.fromtimestamp(t).date


def Time(hour: int, minute: int, second: int) -> datetime:
    return datetime(hour=hour, minute=minute, second=second)


TimeFromTicks = datetime.fromtimestamp

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

    def __init__(self, subtype: type):
        assert not isinstance(subtype, ARRAY), "Invalid array subtype: ARRAY"
        self.subtype = subtype

    def __str__(self):
        return f"Array({self.subtype.__name__})"

    def __eq__(self, other):
        return isinstance(other, ARRAY) and other.subtype == self.subtype


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

    @cached_property
    def python_type(self):
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
        }
        return types[self]


def parse_type(raw_type: str) -> ColType:
    def parse_internal(raw_internal: str) -> ColType:
        try:
            return _InternalType(raw_internal).python_type
        except ValueError:
            # Treat unknown types as strings. Better that error since user still has
            # a way to work with it
            return str

    if raw_type.startswith(ARRAY._prefix) and raw_type.endswith(")"):
        return ARRAY(parse_internal(raw_type[len(ARRAY._prefix) : -1]))

    return parse_internal(raw_type)


DATE_FORMAT: str = "%Y-%m-%d"
DATETIME_FORMAT: str = f"{DATE_FORMAT} %H:%M:%S"


def parse_value(
    value: Union[str, int, bool, float, list],
    ctype: Union[type, ARRAY],
) -> ColType:
    if ctype in (int, str, float):
        return ctype(value)
    if ctype is date:
        assert isinstance(value, str)
        return datetime.strptime(value, DATE_FORMAT).date()
    if ctype is datetime:
        assert isinstance(value, str)
        return datetime.strptime(value, DATETIME_FORMAT)
    if isinstance(ctype, ARRAY):
        return [parse_value(it, ctype.subtype) for it in value]
    raise DataError(f"Unsupported data type returned: {ctype.__name__}")


def format_value(value: ColType) -> str:
    # TODO: FIR-7793
    pass
