from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

from pytest import mark, raises

from firebolt.async_db import (
    ARRAY,
    DECIMAL,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)
from firebolt.async_db._types import parse_type, parse_value
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


def test_parse_value_int() -> None:
    """parse_value parses all int values correctly."""
    assert parse_value(1, int) == 1, "Error parsing integer: provided int"
    assert parse_value("1", int) == 1, "Error parsing integer: provided str"
    assert parse_value(1.1, int) == 1, "Error parsing integer: provided float"
    assert parse_value(None, int) is None, "Error parsing integer: provided None"

    with raises(ValueError):
        parse_value("a", int)

    for val in ((1,), [1], Exception()):
        with raises(TypeError):
            parse_value(val, int)


def test_parse_value_float() -> None:
    """parse_value parses all float values correctly."""
    assert parse_value(1, float) == 1.0, "Error parsing float: provided int"
    assert parse_value("1", float) == 1.0, "Error parsing float: provided str"
    assert parse_value("1.1", float) == 1.1, "Error parsing float: provided str"
    assert parse_value(1.1, float) == 1.1, "Error parsing float: provided float"
    assert parse_value(None, float) is None, "Error parsing float: provided None"

    with raises(ValueError):
        parse_value("a", float)

    for val in ((1.1,), [1.1], Exception()):
        with raises(TypeError):
            parse_value(val, float)


def test_parse_value_str() -> None:
    """parse_value parses all str values correctly."""
    assert parse_value(1, str) == "1", "Error parsing str: provided int"
    assert parse_value("a", str) == "a", "Error parsing str: provided str"
    assert parse_value(1.1, str) == "1.1", "Error parsing str: provided float"
    assert parse_value(("a",), str) == "('a',)", "Error parsing str: provided tuple"
    assert parse_value(["a"], str) == "['a']", "Error parsing str: provided list"
    assert parse_value(None, str) is None, "Error parsing str: provided None"


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


def test_parse_decimal() -> None:
    assert parse_value("123.456", DECIMAL(38, 3)) == Decimal(
        "123.456"
    ), "Error parsing decimal(38, 3): str provided"
    assert parse_value(123, DECIMAL(38, 3)) == Decimal(
        "123"
    ), "Error parsing decimal(38, 3): int provided"
    assert (
        parse_value(None, DECIMAL(38, 3)) is None
    ), "Error parsing decimal(38, 3): None provided"


def test_parse_arrays() -> None:
    """parse_value parses all array values correctly."""
    assert parse_value([1, 2], ARRAY(int)) == [
        1,
        2,
    ], "Error parsing array(int): list[int] provided"
    assert parse_value([1, "2"], ARRAY(int)) == [
        1,
        2,
    ], "Error parsing array(int): mixed list provided"
    assert parse_value(["1", "2"], ARRAY(int)) == [
        1,
        2,
    ], "Error parsing array(int): list[str] provided"

    assert parse_value([1, 2], ARRAY(float)) == [
        1.0,
        2.0,
    ], "Error parsing array(float): list[int] provided"

    assert parse_value(["2021-12-31 23:59:59", "2000-01-01 01:01:01"], ARRAY(str)) == [
        "2021-12-31 23:59:59",
        "2000-01-01 01:01:01",
    ], "Error parsing array(str): list[str] provided"

    assert parse_value(
        ["2021-12-31 23:59:59", "2000-01-01 01:01:01"], ARRAY(datetime)
    ) == [
        datetime(2021, 12, 31, 23, 59, 59),
        datetime(2000, 1, 1, 1, 1, 1),
    ], "Error parsing array(datetime): list[str] provided"

    assert parse_value(["2021-12-31", "2000-01-01"], ARRAY(date)) == [
        date(2021, 12, 31),
        date(2000, 1, 1),
    ], "Error parsing array(datetime): list[str] provided"

    for t in (int, float, str, date, datetime, ARRAY(int)):
        assert (
            parse_value(None, ARRAY(t)) is None
        ), f"Error parsing array({str(t)}): None provided"


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


def test_parse_value_bool() -> None:
    """parse_value parses all int values correctly."""
    assert parse_value(True, bool) == True, "Error parsing boolean: provided true"
    assert parse_value(False, bool) == False, "Error parsing boolean: provided false"
    assert parse_value(2, bool) == True, "Error parsing boolean: provided 2"
    assert parse_value(0, bool) == False, "Error parsing boolean: provided 0"
    assert parse_value(None, int) is None, "Error parsing boolean: provided None"

    with raises(DataError):
        parse_value("true", bool)
