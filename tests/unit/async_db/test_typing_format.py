from datetime import date, datetime, timedelta, timezone

from pytest import mark, raises

from firebolt.async_db import DataError
from firebolt.async_db._types import format_sql, format_value


@mark.parametrize(
    "value,result",
    [  # Strings
        ("abcd", "'abcd'"),
        ("test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'"),
        ("test\\", "'test\\\\'"),
        ("some\0value", "'some\\0value'"),
        # Numbers
        (1, "1"),
        (1.123, "1.123"),
        (True, "1"),
        (False, "0"),
        # Date, datetime
        (date(2022, 1, 10), "'2022-01-10'"),
        (datetime(2022, 1, 10, 1, 1, 1), "'2022-01-10 01:01:01'"),
        (
            datetime(2022, 1, 10, 1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            "'2022-01-10 00:01:01'",
        ),
        # List, tuple
        ([1, 2, 3], "[1, 2, 3]"),
        (("a", "b", "c"), "['a', 'b', 'c']"),
        # None
        (None, "NULL"),
    ],
)
def test_format_value(value: str, result: str) -> None:
    assert format_value(value) == result, "Invalid format_value result"


def test_format_value_errors() -> None:
    with raises(DataError) as exc_info:
        format_value(Exception())

    assert str(exc_info.value) == "unsupported parameter type <class 'Exception'>"


@mark.parametrize(
    "sql,params,result",
    [
        ("text", (), "text"),
        ("?", (1,), "1"),
        ("?, \\?", (1,), "1, ?"),
        ("\\\\?", (), "\\?"),
        ("\\??", (1,), "?1"),
        ("??", (1, 2), "12"),
        ("\\\\??", (1,), "\\?1"),
    ],
)
def test_format_sql(sql: str, params: tuple, result: str) -> None:
    assert format_sql(sql, params) == result, "Invalid format sql result"


def test_format_sql_errors() -> None:
    with raises(DataError) as exc_info:
        format_sql("?", [])
    assert (
        str(exc_info.value)
        == "not enough parameters provided for substitution: given 0, found one more at"
        " position 0"
    ), "Invalid not enought parameters error"

    with raises(DataError) as exc_info:
        format_sql("?", (1, 2))
    assert (
        str(exc_info.value)
        == "too many parameters provided for substitution: given 2, used only 1"
    ), "Invalid not enought parameters error"
