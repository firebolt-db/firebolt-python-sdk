from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Sequence, Union

from sqlparse import parse as parse_sql  # type: ignore
from sqlparse.sql import (  # type: ignore
    Comparison,
    Statement,
    Token,
    TokenList,
)
from sqlparse.tokens import Token as TokenType  # type: ignore

try:
    from ciso8601 import parse_datetime  # type: ignore
except ImportError:
    # Unfortunately, there seems to be no support for optional bits in strptime
    def parse_datetime(date_string: str) -> datetime:  # type: ignore
        format = "%Y-%m-%d %H:%M:%S.%f"
        # fromisoformat doesn't support milliseconds
        if "." in date_string:
            return datetime.strptime(date_string, format)
        return datetime.fromisoformat(date_string)


from firebolt.utils.exception import (
    DataError,
    InterfaceError,
    NotSupportedError,
)
from firebolt.utils.util import cached_property

_NoneType = type(None)
_col_types = (int, float, str, datetime, date, bool, list, Decimal, _NoneType)
# duplicating this since 3.7 can't unpack Union
ColType = Union[int, float, str, datetime, date, bool, list, Decimal, _NoneType]
RawColType = Union[int, float, str, bool, list, _NoneType]
ParameterType = Union[int, float, str, datetime, date, bool, Decimal, Sequence]

# These definitions are required by PEP-249
Date = date


def DateFromTicks(t: int) -> date:
    """Convert `ticks` to `date` for Firebolt DB."""
    return datetime.fromtimestamp(t).date()


def Time(hour: int, minute: int, second: int) -> None:
    """Unsupported: Construct `time`, for Firebolt DB."""
    raise NotSupportedError("The time construct is not supported by Firebolt")


def TimeFromTicks(t: int) -> None:
    """Unsupported: Convert `ticks` to `time` for Firebolt DB."""
    raise NotSupportedError("The time construct is not supported by Firebolt")


Timestamp = datetime
TimestampFromTicks = datetime.fromtimestamp


def Binary(value: str) -> str:
    """Convert string to binary for Firebolt DB does nothing."""
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
    """Class for holding `array` column type information in Firebolt DB."""

    _prefix = "Array("

    def __init__(self, subtype: Union[type, ARRAY, DECIMAL, DATETIME64]):
        assert (subtype in _col_types and subtype is not list) or isinstance(
            subtype, (ARRAY, DECIMAL, DATETIME64)
        ), f"Invalid array subtype: {str(subtype)}"
        self.subtype = subtype

    def __str__(self) -> str:
        return f"Array({str(self.subtype)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ARRAY):
            return NotImplemented
        return other.subtype == self.subtype


class DECIMAL:
    """Class for holding `decimal` value information in Firebolt DB."""

    _prefix = "Decimal("

    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale

    def __str__(self) -> str:
        return f"Decimal({self.precision}, {self.scale})"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DECIMAL):
            return NotImplemented
        return other.precision == self.precision and other.scale == self.scale


class DATETIME64:
    """Class for holding `datetime64` value information in Firebolt DB."""

    _prefix = "DateTime64("

    def __init__(self, precision: int):
        self.precision = precision

    def __str__(self) -> str:
        return f"DateTime64({self.precision})"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DATETIME64):
            return NotImplemented
        return other.precision == self.precision


NULLABLE_PREFIX = "Nullable("


class _InternalType(Enum):
    """Enum of all internal Firebolt types, except for `array`."""

    # INT, INTEGER
    Int8 = "Int8"
    UInt8 = "UInt8"
    Int16 = "Int16"
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
    Date32 = "Date32"

    # DATETIME, TIMESTAMP
    DateTime = "DateTime"

    # Nullable(Nothing)
    Nothing = "Nothing"

    @cached_property
    def python_type(self) -> type:
        """Convert internal type to Python type."""
        types = {
            _InternalType.Int8: int,
            _InternalType.UInt8: int,
            _InternalType.Int16: int,
            _InternalType.UInt16: int,
            _InternalType.Int32: int,
            _InternalType.UInt32: int,
            _InternalType.Int64: int,
            _InternalType.UInt64: int,
            _InternalType.Float32: float,
            _InternalType.Float64: float,
            _InternalType.String: str,
            _InternalType.Date: date,
            _InternalType.Date32: date,
            _InternalType.DateTime: datetime,
            # For simplicity, this could happen only during 'select null' query
            _InternalType.Nothing: str,
        }
        return types[self]


def parse_type(raw_type: str) -> Union[type, ARRAY, DECIMAL, DATETIME64]:  # noqa: C901
    """Parse typename provided by query metadata into Python type."""
    if not isinstance(raw_type, str):
        raise DataError(f"Invalid typename {str(raw_type)}: str expected")
    # Handle arrays
    if raw_type.startswith(ARRAY._prefix) and raw_type.endswith(")"):
        return ARRAY(parse_type(raw_type[len(ARRAY._prefix) : -1]))
    # Handle decimal
    if raw_type.startswith(DECIMAL._prefix) and raw_type.endswith(")"):
        try:
            prec_scale = raw_type[len(DECIMAL._prefix) : -1].split(",")
            precision, scale = int(prec_scale[0]), int(prec_scale[1])
        except (ValueError, IndexError):
            pass
        else:
            return DECIMAL(precision, scale)
    # Handle detetime64
    if raw_type.startswith(DATETIME64._prefix) and raw_type.endswith(")"):
        try:
            precision = int(raw_type[len(DATETIME64._prefix) : -1])
        except (ValueError, IndexError):
            pass
        else:
            return DATETIME64(precision)
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
    ctype: Union[type, ARRAY, DECIMAL, DATETIME64],
) -> ColType:
    """Provided raw value, and Python type; parses first into Python value."""
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
    if ctype is datetime or isinstance(ctype, DATETIME64):
        if not isinstance(value, str):
            raise DataError(f"Invalid datetime value {value}: str expected")
        return parse_datetime(value)
    if isinstance(ctype, DECIMAL):
        assert isinstance(value, (str, int))
        return Decimal(value)
    if isinstance(ctype, ARRAY):
        assert isinstance(value, list)
        return [parse_value(it, ctype.subtype) for it in value]
    raise DataError(f"Unsupported data type returned: {ctype.__name__}")


escape_chars = {
    "\0": "\\0",
    "\\": "\\\\",
    "'": "\\'",
}


def format_value(value: ParameterType) -> str:
    """For Python value to be used in a SQL query."""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    elif isinstance(value, str):
        return f"'{''.join(escape_chars.get(c, c) for c in value)}'"
    elif isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(value, date):
        return f"'{value.isoformat()}'"
    if value is None:
        return "NULL"
    elif isinstance(value, Sequence):
        return f"[{', '.join(format_value(it) for it in value)}]"

    raise DataError(f"unsupported parameter type {type(value)}")


def format_statement(statement: Statement, parameters: Sequence[ParameterType]) -> str:
    """
    Substitute placeholders in a `sqlparse` statement with provided values.
    """
    idx = 0

    def process_token(token: Token) -> Token:
        nonlocal idx
        if token.ttype == TokenType.Name.Placeholder:
            # Replace placeholder with formatted parameter
            if idx >= len(parameters):
                raise DataError(
                    "not enough parameters provided for substitution: given "
                    f"{len(parameters)}, found one more"
                )
            formatted = format_value(parameters[idx])
            idx += 1
            return Token(TokenType.Text, formatted)
        if isinstance(token, TokenList):
            # Process all children tokens

            return TokenList([process_token(t) for t in token.tokens])
        return token

    formatted_sql = statement_to_sql(process_token(statement))

    if idx < len(parameters):
        raise DataError(
            f"too many parameters provided for substitution: given {len(parameters)}, "
            f"used only {idx}"
        )

    return formatted_sql


SetParameter = namedtuple("SetParameter", ["name", "value"])


def statement_to_set(statement: Statement) -> Optional[SetParameter]:
    """
    Try to parse `statement` as a `SET` command.
    Return `None` if it's not a `SET` command.
    """
    # Filter out meaningless tokens like Punctuation and Whitespaces
    tokens = [
        token
        for token in statement.tokens
        if token.ttype == TokenType.Keyword or isinstance(token, Comparison)
    ]

    # Check if it's a SET statement by checking if it starts with set
    if (
        len(tokens) > 0
        and tokens[0].ttype == TokenType.Keyword
        and tokens[0].value.lower() == "set"
    ):
        # Check if set statement has a valid format
        if len(tokens) != 2 or not isinstance(tokens[1], Comparison):
            raise InterfaceError(
                f"Invalid set statement format: {statement_to_sql(statement)},"
                " expected SET <param> = <value>"
            )
        return SetParameter(
            statement_to_sql(tokens[1].left), statement_to_sql(tokens[1].right)
        )
    return None


def statement_to_sql(statement: Statement) -> str:
    return str(statement).strip().rstrip(";")


def split_format_sql(
    query: str, parameters: Sequence[Sequence[ParameterType]]
) -> List[Union[str, SetParameter]]:
    """
    Multi-statement query formatting will result in `NotSupportedError`.
    Instead, split a query into a separate statement and format with parameters.
    """
    statements = parse_sql(query)
    if not statements:
        return [query]

    if parameters:
        if len(statements) > 1:
            raise NotSupportedError(
                "Formatting multi-statement queries is not supported."
            )
        if statement_to_set(statements[0]):
            raise NotSupportedError("Formatting set statements is not supported.")
        return [format_statement(statements[0], paramset) for paramset in parameters]

    # Try parsing each statement as a SET, otherwise return as a plain sql string
    return [statement_to_set(st) or statement_to_sql(st) for st in statements]
