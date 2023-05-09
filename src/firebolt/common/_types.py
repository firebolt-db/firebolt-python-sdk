from __future__ import annotations

import re
from collections import namedtuple
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Sequence, Union

from sqlparse import parse as parse_sql  # type: ignore
from sqlparse.sql import (  # type: ignore
    Comment,
    Comparison,
    Statement,
    Token,
    TokenList,
)
from sqlparse.tokens import Comparison as ComparisonType  # type: ignore
from sqlparse.tokens import Newline  # type: ignore
from sqlparse.tokens import Whitespace  # type: ignore
from sqlparse.tokens import Token as TokenType  # type: ignore

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


from firebolt.utils.exception import (
    DataError,
    InterfaceError,
    NotSupportedError,
)
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

    __name__ = "Array"
    _prefix = "array("

    def __init__(self, subtype: Union[type, ARRAY, DECIMAL]):
        assert (subtype in _col_types and subtype is not list) or isinstance(
            subtype, (ARRAY, DECIMAL)
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

    __name__ = "Decimal"
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


NULLABLE_SUFFIX = "null"


class _InternalType(Enum):
    """Enum of all internal Firebolt types, except for `array`."""

    Int = "int"
    Long = "long"
    Float = "float"
    Double = "double"

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

    Nothing = "Nothing"

    @cached_property
    def python_type(self) -> type:
        """Convert internal type to Python type."""
        types = {
            _InternalType.Int: int,
            _InternalType.Long: int,
            _InternalType.Float: float,
            _InternalType.Double: float,
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
            # For simplicity, this could happen only during 'select null' query
            _InternalType.Nothing: str,
        }
        return types[self]


def parse_type(raw_type: str) -> Union[type, ARRAY, DECIMAL]:  # noqa: C901
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
    ctype: Union[type, ARRAY, DECIMAL],
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
        return "true" if value else "false"
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
    elif isinstance(value, bytes):
        # Encode each byte into hex
        return "'" + "".join(f"\\x{b:02x}" for b in value) + "'"
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
    skip_types = [Whitespace, Newline]
    tokens = [
        token
        for token in statement.tokens
        if token.ttype not in skip_types and not isinstance(token, Comment)
    ]
    # Trim tail punctuation
    right_idx = len(tokens) - 1
    while str(tokens[right_idx]) == ";":
        right_idx -= 1

    tokens = tokens[: right_idx + 1]

    # Check if it's a SET statement by checking if it starts with set
    if (
        len(tokens) > 0
        and tokens[0].ttype == TokenType.Keyword
        and tokens[0].value.lower() == "set"
    ):
        # Check if set statement has a valid format
        if len(tokens) == 2 and isinstance(tokens[1], Comparison):
            return SetParameter(
                statement_to_sql(tokens[1].left),
                statement_to_sql(tokens[1].right).strip("'"),
            )
        # Or if at least there is a comparison
        cmp_idx = next(
            (
                i
                for i, token in enumerate(tokens)
                if token.ttype == ComparisonType or isinstance(token, Comparison)
            ),
            None,
        )
        if cmp_idx:
            left_tokens, right_tokens = tokens[1:cmp_idx], tokens[cmp_idx + 1 :]
            if isinstance(tokens[cmp_idx], Comparison):
                left_tokens = left_tokens + [tokens[cmp_idx].left]
                right_tokens = [tokens[cmp_idx].right] + right_tokens

            if left_tokens and right_tokens:
                return SetParameter(
                    "".join(statement_to_sql(t) for t in left_tokens),
                    "".join(statement_to_sql(t) for t in right_tokens).strip("'"),
                )

        raise InterfaceError(
            f"Invalid set statement format: {statement_to_sql(statement)},"
            " expected SET <param> = <value>"
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
