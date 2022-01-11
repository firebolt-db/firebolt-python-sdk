from datetime import date, datetime, timedelta, timezone

from pytest import raises

from firebolt.async_db import DataError
from firebolt.async_db._types import format_sql, format_value


def test_format_value() -> None:
    test_cases = (
        # Strings
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
    )

    for value, result in test_cases:
        assert format_value(value) == result, "Invalid format_value result"

    with raises(DataError) as exc_info:
        format_value(Exception())

    assert str(exc_info.value) == "unsupported parameter type <class 'Exception'>"


def test_format_sql() -> None:
    test_cases = (
        ("text", (), "text"),
        ("?", (1,), "1"),
        ("?, \\?", (1,), "1, ?"),
        ("\\\\?", (), "\\?"),
        ("\\??", (1,), "?1"),
        ("??", (1, 2), "12"),
        ("\\\\??", (1,), "\\?1"),
    )

    for sql, params, result in test_cases:
        assert format_sql(sql, params) == result, "Invalid format sql result"

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
