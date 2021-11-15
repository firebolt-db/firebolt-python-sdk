from datetime import date, datetime
from typing import Dict

from pytest import raises

from firebolt.async_db import (
    ARRAY,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)
from firebolt.async_db._types import parse_type, parse_value
from firebolt.common.exception import DataError, NotSupportedError


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


def test_parse_value_datetime() -> None:
    """parse_value parses all date and datetime values correctly."""
    # Date
    assert parse_value("2021-12-31", date) == date(
        2021, 12, 31
    ), "Error parsing date: str provided"
    assert parse_value(None, date) is None, "Error parsing date: None provided"

    assert parse_value("2021-12-31 23:59:59", date) == date(
        2021, 12, 31
    ), "Error parsing date: datetime string provided"

    with raises(ValueError):
        parse_value("abd", date)

    for value in ([2021, 12, 31], (2021, 12, 31)):
        with raises(DataError) as exc_info:
            parse_value(value, date)

        assert str(exc_info.value) == f"Invalid date value {value}: str expected"

    # Datetime
    assert parse_value("2021-12-31 23:59:59", datetime) == datetime(
        2021, 12, 31, 23, 59, 59
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
