import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

from pytest import mark, raises

from firebolt.common._types import (
    ARRAY,
    DECIMAL,
    STRUCT,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
    parse_type,
    parse_value,
    split_struct_fields,
)
from firebolt.utils.exception import DataError, NotSupportedError


def test_parse_type(types_map: Dict[str, type]) -> None:
    """parse_type function parses all internal types correctly."""
    for type_name, t in types_map.items():
        parsed = parse_type(type_name)
        assert (
            parsed == t
        ), f"Error parsing type {type_name}: expected {str(t)}, got {str(parsed)}"

    with raises(DataError) as exc_info:
        parse_type(1)

    assert (
        str(exc_info.value) == "Invalid typename 1: str expected"
    ), "Invalid type parsing error message"


def test_parse_struct_type_with_spaces() -> None:
    parsed = parse_type("struct(`a b` int, s struct(`c d` text))")
    assert parsed == STRUCT(
        {"a b": int, "s": STRUCT({"c d": str})}
    ), f"Error parsing struct type with spaces"


@mark.parametrize(
    "value,expected,error",
    [
        (1, 1, None),
        ("1", 1, None),
        (1.1, 1, None),
        (None, None, None),
        ("a", None, ValueError),
        ((1,), None, TypeError),
        ([1], None, TypeError),
        (Exception(), None, TypeError),
    ],
)
def test_parse_value_int(value, expected, error) -> None:
    """parse_value parses all int values correctly."""
    if error:
        with raises(error):
            parse_value(value, int)
    else:
        assert (
            parse_value(value, int) == expected
        ), f"Error parsing integer: provided {value}, expected {expected}"


@mark.parametrize(
    "value,expected,error",
    [
        (1, 1.0, None),
        ("1", 1.0, None),
        ("1.1", 1.1, None),
        (1.1, 1.1, None),
        (None, None, None),
        ("inf", float("inf"), None),
        ("-inf", float("-inf"), None),
        ("nan", float("nan"), None),
        ("-nan", float("nan"), None),
        ("a", None, ValueError),
        ((1.1,), None, TypeError),
        ([1.1], None, TypeError),
        (Exception(), None, TypeError),
    ],
)
def test_parse_value_float(value, expected, error) -> None:
    """parse_value parses all float values correctly."""
    if error:
        with raises(error):
            parse_value(value, float)
    else:
        if expected and math.isnan(expected):
            assert math.isnan(
                parse_value(value, float)
            ), f"Error parsing float: provided {value}, expected {expected}"
        else:
            assert (
                parse_value(value, float) == expected
            ), f"Error parsing float: provided {value}, expected {expected}"


@mark.parametrize(
    "value,expected",
    [
        (1, "1"),
        ("a", "a"),
        (1.1, "1.1"),
        (("a",), "('a',)"),
        (["a"], "['a']"),
        (None, None),
    ],
)
def test_parse_value_str(value, expected) -> None:
    """parse_value parses all str values correctly."""
    assert (
        parse_value(value, str) == expected
    ), f"Error parsing str: provided {value}, expected {expected}"


@mark.parametrize(
    "value,expected,case",
    [
        ("2021-12-31", date(2021, 12, 31), "str provided"),
        ("0001-01-01", date(1, 1, 1), "range low provided"),
        ("9999-12-31", date(9999, 12, 31), "range high provided"),
        (None, None, "None provided"),
        ("2021-12-31 23:59:59", date(2021, 12, 31), "datetime provided"),
    ],
)
def test_parse_value_date(value: Optional[str], expected: Optional[date], case: str):
    """parse_value parses all date values correctly."""
    assert parse_value(value, date) == expected, f"Error parsing date: {case}"


@mark.parametrize(
    "value,expected,case",
    [
        (
            "2021-12-31 23:59:59.1234",
            datetime(2021, 12, 31, 23, 59, 59, 123400),
            "str provided",
        ),
        (
            "0001-01-01 00:00:00.000000",
            datetime(1, 1, 1, 0, 0, 0, 0),
            "range low provided",
        ),
        (
            "9999-12-31 23:59:59.999999",
            datetime(9999, 12, 31, 23, 59, 59, 999999),
            "range high provided",
        ),
        (
            "2021-12-31 23:59:59.1234-03",
            datetime(
                2021, 12, 31, 23, 59, 59, 123400, tzinfo=timezone(timedelta(hours=-3))
            ),
            "timezone provided",
        ),
        (
            "2021-12-31 23:59:59.1234+05:30:12",
            datetime(
                2021,
                12,
                31,
                23,
                59,
                59,
                123400,
                tzinfo=timezone(timedelta(hours=5, minutes=30, seconds=12)),
            ),
            "timezone with seconds provided",
        ),
        (None, None, "None provided"),
        ("2021-12-31", datetime(2021, 12, 31), "date provided"),
    ],
)
def test_parse_value_datetime(
    value: Optional[str], expected: Optional[date], case: str
):
    """parse_value parses all date values correctly."""
    assert parse_value(value, datetime) == expected, f"Error parsing datetime: {case}"


def test_parse_value_datetime_errors() -> None:
    """parse_value parses all date and datetime values correctly."""
    with raises(ValueError):
        parse_value("abd", date)

    for value in ([2021, 12, 31], (2021, 12, 31)):
        with raises(DataError) as exc_info:
            parse_value(value, date)

        assert str(exc_info.value) == f"Invalid date value {value}: str expected"

    # Datetime
    assert parse_value("2021-12-31 23:59:59.1234", datetime) == datetime(
        2021, 12, 31, 23, 59, 59, 123400
    ), "Error parsing datetime: str provided"
    assert parse_value(None, datetime) is None, "Error parsing datetime: None provided"

    assert parse_value("2021-12-31", datetime) == datetime(
        2021, 12, 31
    ), "Error parsing datetime: date string provided"

    with raises(ValueError):
        parse_value("abd", datetime)

    for value in ([2021, 12, 31], (2021, 12, 31)):
        with raises(DataError) as exc_info:
            parse_value(value, datetime)

        assert str(exc_info.value) == f"Invalid datetime value {value}: str expected"


@mark.parametrize(
    "value,expected",
    [
        ("123.456", Decimal("123.456")),
        (123, Decimal("123")),
        (None, None),
    ],
)
def test_parse_decimal(value, expected) -> None:
    assert (
        parse_value(value, DECIMAL(38, 3)) == expected
    ), f"Error parsing decimal(38, 3): provided {value}, expected {expected}"


@mark.parametrize(
    "value,expected,type",
    [
        ([1, 2], [1, 2], int),
        ([1, "2"], [1, 2], int),
        (["1", "2"], [1, 2], int),
        ([1, 2], [1.0, 2.0], float),
        (
            ["2021-12-31 23:59:59", "2000-01-01 00:00:00"],
            ["2021-12-31 23:59:59", "2000-01-01 00:00:00"],
            str,
        ),
        (
            ["2021-12-31 23:59:59", "2000-01-01 00:00:00"],
            [datetime(2021, 12, 31, 23, 59, 59), datetime(2000, 1, 1, 0, 0, 0)],
            datetime,
        ),
        (["2021-12-31", "2000-01-01"], [date(2021, 12, 31), date(2000, 1, 1)], date),
        (None, None, int),
        (None, None, float),
        (None, None, str),
        (None, None, datetime),
        (None, None, date),
        (None, None, ARRAY(int)),
        ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}], STRUCT({"a": int})),
    ],
)
def test_parse_arrays(value, expected, type) -> None:
    assert (
        parse_value(value, ARRAY(type)) == expected
    ), f"Error parsing array({type}): provided {value}, expected {expected}"


def test_helpers() -> None:
    """All provided helper functions work properly."""
    d = date(2021, 12, 31)
    dts = datetime(d.year, d.month, d.day).timestamp()
    assert DateFromTicks(dts) == d, "Error running DateFromTicks"

    dt = datetime(2021, 12, 31, 23, 59, 59)
    assert (
        TimestampFromTicks(datetime.timestamp(dt)) == dt
    ), "Error running TimestampFromTicks"

    with raises(NotSupportedError):
        TimeFromTicks(0)


@mark.parametrize(
    "value,expected,error",
    [
        (True, True, None),
        (False, False, None),
        (2, True, None),
        (0, False, None),
        (None, None, None),
        ("true", None, DataError),
    ],
)
def test_parse_value_bool(value, expected, error) -> None:
    """parse_value parses all int values correctly."""
    if error:
        with raises(error):
            parse_value(value, bool)
    else:
        assert (
            parse_value(value, bool) == expected
        ), f"Error parsing boolean: provided {value}"


@mark.parametrize(
    "value,expected,error",
    [
        ("\\x616263", b"abc", None),
        (None, None, None),
        ("\\xabc", None, ValueError),
        ("616263", None, ValueError),
        (1, None, DataError),
    ],
)
def test_parse_value_bytes(value, expected, error) -> None:
    """parse_value parses all int values correctly."""
    if error:
        with raises(error):
            parse_value(value, bytes)
    else:
        assert (
            parse_value(value, bytes) == expected
        ), f"Error parsing bytes: provided {value}"


@mark.parametrize(
    "value,expected,type_,error",
    [
        (
            {"a": 1, "b": False},
            {"a": 1, "b": False},
            STRUCT({"a": int, "b": bool}),
            None,
        ),
        (
            {"a": 1, "b": "a"},
            {"a": 1, "b": "1"},
            STRUCT({"a": int, "b": bool}),
            DataError,
        ),
        (
            {"dt": "2021-12-31 23:59:59", "d": "2021-12-31"},
            {"dt": datetime(2021, 12, 31, 23, 59, 59), "d": date(2021, 12, 31)},
            STRUCT({"dt": datetime, "d": date}),
            None,
        ),
        (
            {"a": 1, "s": {"b": "2021-12-31"}},
            {"a": 1, "s": {"b": date(2021, 12, 31)}},
            STRUCT({"a": int, "s": STRUCT({"b": date})}),
            None,
        ),
        (
            {"a": None, "b": None},
            {"a": None, "b": None},
            STRUCT({"a": int, "b": bool}),
            None,
        ),
        (None, None, STRUCT({"a": int, "b": bool}), None),
        ({"a": [1, 2, 3]}, {"a": [1, 2, 3]}, STRUCT({"a": ARRAY(int)}), None),
    ],
)
def test_parse_value_struct(value, expected, type_, error) -> None:
    """parse_value parses all int values correctly."""
    if error:
        with raises(error):
            parse_value(value, type_)
    else:
        assert (
            parse_value(value, type_) == expected
        ), f"Error parsing struct: provided {value}"


@mark.parametrize(
    "value,expected",
    [
        ("a int, b text", ["a int", "b text"]),
        ("a int, s struct(a int, b text)", ["a int", "s struct(a int, b text)"]),
        ("a int, b array(struct(a int))", ["a int", "b array(struct(a int))"]),
    ],
)
def test_split_struct_fields(value, expected) -> None:
    assert (
        split_struct_fields(value) == expected
    ), f"Error splitting struct fields: provided {value}, expected {expected}"
