from __future__ import annotations

import re
from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from io import StringIO
from typing import Any, Dict, List, Sequence, Tuple, Union

try:
    from ciso8601 import parse_datetime  # type: ignore
except ImportError:
    unsupported_milliseconds_re = re.compile(r"(?<=\.)\d{1,5}(?!\d)")

    def _fix_milliseconds(datetime_string: str) -> str:
        # Fill milliseconds with 0 to have exactly 6 digits
        # Python parser only supports 3 or 6 digit milliseconds untill 3.11
        def align_ms(match: re.Match) -> str:
            ms = match.group()
            return ms + "0" * (6 - len(ms))

        return re.sub(unsupported_milliseconds_re, align_ms, datetime_string)

    def _fix_timezone(datetime_string: str) -> str:
        # timezone, provided as +/-dd is not supported by datetime.
        # We need to append :00 to it
        if datetime_string[-3] in "+-":
            return datetime_string + ":00"
        return datetime_string

    def parse_datetime(datetime_string: str) -> datetime:
        return datetime.fromisoformat(_fix_timezone(_fix_milliseconds(datetime_string)))


from firebolt.utils.exception import DataError, NotSupportedError
from firebolt.utils.util import cached_property

_NoneType = type(None)
_col_types = (int, float, str, datetime, date, bool, list, Decimal, _NoneType, bytes)
# duplicating this since 3.7 can't unpack Union
ColType = Union[int, float, str, datetime, date, bool, list, Decimal, _NoneType, bytes]
RawColType = Union[int, float, str, bool, list, _NoneType]
ParameterType = Union[int, float, str, datetime, date, bool, Decimal, Sequence, bytes]

# These definitions are required by PEP-249
Date = date


def DateFromTicks(t: int) -> date:  # NOSONAR
    """Convert `ticks` to `date` for Firebolt DB."""
    return datetime.fromtimestamp(t).date()


def Time(hour: int, minute: int, second: int) -> None:  # NOSONAR
    """Unsupported: Construct `time`, for Firebolt DB."""
    raise NotSupportedError("The time construct is not supported by Firebolt")


def TimeFromTicks(t: int) -> None:  # NOSONAR
    """Unsupported: Convert `ticks` to `time` for Firebolt DB."""
    raise NotSupportedError("The time construct is not supported by Firebolt")


Timestamp = datetime
TimestampFromTicks = datetime.fromtimestamp


def Binary(value: str) -> bytes:  # NOSONAR
    """Encode a string into UTF-8."""
    return value.encode("utf-8")


STRING = str
BINARY = bytes
NUMBER = int
DATETIME = datetime
ROWID = int


class ExtendedType:
    """Base type for all extended types in Firebolt (array, decimal, struct, etc.)."""

    __name__ = "ExtendedType"

    @staticmethod
    def is_valid_type(type_: Any) -> bool:
        return type_ in _col_types or isinstance(type_, ExtendedType)

    # Remember to override this method in subclasses
    # if __eq__ is overridden
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    def __hash__(self) -> int:
        return hash(str(self))


class ARRAY(ExtendedType):
    """Class for holding `array` column type information in Firebolt DB."""

    __name__ = "Array"
    _prefix = "array("

    def __init__(self, subtype: Union[type, ExtendedType]):
        if not self.is_valid_type(subtype):
            raise ValueError(f"Invalid array subtype: {str(subtype)}")
        self.subtype = subtype

    def __str__(self) -> str:
        return f"Array({str(self.subtype)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ARRAY):
            return NotImplemented
        return other.subtype == self.subtype

    __hash__ = ExtendedType.__hash__


class DECIMAL(ExtendedType):
    """Class for holding `decimal` value information in Firebolt DB."""

    __name__ = "Decimal"
    _prefixes = ["Decimal(", "numeric("]

    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale

    def __str__(self) -> str:
        return f"Decimal({self.precision}, {self.scale})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DECIMAL):
            return NotImplemented
        return other.precision == self.precision and other.scale == self.scale

    __hash__ = ExtendedType.__hash__


class STRUCT(ExtendedType):
    __name__ = "Struct"
    _prefix = "struct("

    def __init__(self, fields: Dict[str, Union[type, ExtendedType]]):
        for name, type_ in fields.items():
            if not self.is_valid_type(type_):
                raise ValueError(f"Invalid struct field type: {str(type_)}")
        self.fields = fields

    def __str__(self) -> str:
        return f"Struct({', '.join(f'{k}: {v}' for k, v in self.fields.items())})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, STRUCT) and other.fields == self.fields

    __hash__ = ExtendedType.__hash__


NULLABLE_SUFFIX = "null"


class _InternalType(Enum):
    """Enum of all internal Firebolt types, except for `array`."""

    Int = "int"
    Integer = "integer"
    Long = "long"
    BigInt = "bigint"
    Float = "float"
    Double = "double"
    DoublePrecision = "double precision"

    Text = "text"

    Date = "date"
    DateExt = "date_ext"
    PGDate = "pgdate"

    Timestamp = "timestamp"
    TimestampExt = "timestamp_ext"
    TimestampNtz = "timestampntz"
    TimestampTz = "timestamptz"

    Boolean = "boolean"

    Bytea = "bytea"
    Geography = "geography"

    Nothing = "Nothing"

    @cached_property
    def python_type(self) -> type:
        """Convert internal type to Python type."""
        types = {
            _InternalType.Int: int,
            _InternalType.Integer: int,
            _InternalType.Long: int,
            _InternalType.BigInt: int,
            _InternalType.Float: float,
            _InternalType.Double: float,
            _InternalType.DoublePrecision: float,
            _InternalType.Text: str,
            _InternalType.Date: date,
            _InternalType.DateExt: date,
            _InternalType.PGDate: date,
            _InternalType.Timestamp: datetime,
            _InternalType.TimestampExt: datetime,
            _InternalType.TimestampNtz: datetime,
            _InternalType.TimestampTz: datetime,
            _InternalType.Boolean: bool,
            _InternalType.Bytea: bytes,
            _InternalType.Geography: str,
            # For simplicity, this could happen only during 'select null' query
            _InternalType.Nothing: str,
        }
        return types[self]


def split_struct_fields(raw_struct: str) -> List[str]:
    """Split raw struct inner fields string into a list of field definitions.
    >>> split_struct_fields("field1 int, field2 struct(field1 int, field2 text)")
    ['field1 int', 'field2 struct(field1 int, field2 text)']
    """
    balance = 0  # keep track of the level of nesting, and only split on level 0
    separator = ","
    res = []
    current = StringIO()
    for i, ch in enumerate(raw_struct):
        if ch == "(":
            balance += 1
        elif ch == ")":
            balance -= 1
        elif ch == separator and balance == 0:
            res.append(current.getvalue().strip())
            current = StringIO()
            continue
        current.write(ch)

    res.append(current.getvalue().strip())
    return res


def split_struct_field(raw_field: str) -> Tuple[str, str]:
    """Split raw struct field into name and type.
    >>> split_struct_field("field int")
    ('field', 'int')
    >>> split_struct_field("`with space` text null")
    ('with space', 'text null')
    >>> split_struct_field("s struct(`a b` int)")
    ('s', 'struct(`a b` int)')
    """
    raw_field = raw_field.strip()
    second_tick = (
        raw_field.find("`", raw_field.find("`") + 1)
        if raw_field.startswith("`")
        else -1
    )
    name, type_ = (
        (raw_field[: second_tick + 1], raw_field[second_tick + 1 :])
        if second_tick != -1
        else raw_field.split(" ", 1)
    )
    return name.strip(" `"), type_.strip()


def parse_type(raw_type: str) -> Union[type, ExtendedType]:  # noqa: C901
    """Parse typename provided by query metadata into Python type."""
    if not isinstance(raw_type, str):
        raise DataError(f"Invalid typename {str(raw_type)}: str expected")
    # Handle arrays
    if raw_type.startswith(ARRAY._prefix) and raw_type.endswith(")"):
        return ARRAY(parse_type(raw_type[len(ARRAY._prefix) : -1]))
    # Handle decimal
    for prefix in DECIMAL._prefixes:
        if raw_type.startswith(prefix) and raw_type.endswith(")"):
            try:
                prec_scale = raw_type[len(prefix) : -1].split(",")
                precision, scale = int(prec_scale[0]), int(prec_scale[1])
                return DECIMAL(precision, scale)
            except (ValueError, IndexError):
                pass
    # Handle structs
    if raw_type.startswith(STRUCT._prefix) and raw_type.endswith(")"):
        try:
            fields_raw = split_struct_fields(raw_type[len(STRUCT._prefix) : -1])
            fields = {}
            for f in fields_raw:
                name, type_ = split_struct_field(f)
                fields[name.strip()] = parse_type(type_.strip())
            return STRUCT(fields)
        except ValueError:
            pass
    # Handle nullable
    if raw_type.endswith(NULLABLE_SUFFIX):
        return parse_type(raw_type[: -len(NULLABLE_SUFFIX)].strip(" "))
    try:
        return _InternalType(raw_type).python_type
    except ValueError:
        # Treat unknown types as strings. Better that error since user still has
        # a way to work with it
        return str


BYTEA_PREFIX = "\\x"


def _parse_bytea(str_value: str) -> bytes:
    if (
        len(str_value) < len(BYTEA_PREFIX)
        or str_value[: len(BYTEA_PREFIX)] != BYTEA_PREFIX
    ):
        raise ValueError(f"Invalid bytea value format: {BYTEA_PREFIX} prefix expected")
    return bytes.fromhex(str_value[len(BYTEA_PREFIX) :])


def parse_value(
    value: RawColType,
    ctype: Union[type, ExtendedType],
) -> ColType:
    """Provided raw value, and Python type; parses first into Python value."""
    if value is None:
        return None
    if ctype in (int, str, float):
        assert isinstance(ctype, type)  # assertion for mypy
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
    if ctype is bool:
        if not isinstance(value, (bool, int)):
            raise DataError(f"Invalid boolean value {value}: bool or int expected")
        return bool(value)
    if ctype is bytes:
        if not isinstance(value, str):
            raise DataError(f"Invalid bytea value {value}: str expected")
        return _parse_bytea(value)
    if isinstance(ctype, DECIMAL):
        if not isinstance(value, (str, int)):
            raise DataError(f"Invalid decimal value {value}: str or int expected")
        return Decimal(value)
    if isinstance(ctype, ARRAY):
        if not isinstance(value, list):
            raise DataError(f"Invalid array value {value}: list expected")
        return [parse_value(it, ctype.subtype) for it in value]
    if isinstance(ctype, STRUCT):
        if not isinstance(value, dict):
            raise DataError(f"Invalid struct value {value}: dict expected")
        return {
            name: parse_value(value.get(name), type_)
            for name, type_ in ctype.fields.items()
        }
    raise DataError(f"Unsupported data type returned: {ctype.__name__}")


SetParameter = namedtuple("SetParameter", ["name", "value"])
