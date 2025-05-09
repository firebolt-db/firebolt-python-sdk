from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional

from pytest import fixture, mark, raises
from sqlparse import parse
from sqlparse.sql import Statement

from firebolt.async_db import (
    Binary,
    DataError,
    InterfaceError,
    NotSupportedError,
)
from firebolt.common._types import SetParameter
from firebolt.common.statement_formatter import (
    StatementFormatter,
    create_statement_formatter,
)


@fixture
def formatter() -> StatementFormatter:
    return create_statement_formatter(version=2)


@fixture
def formatter_v1() -> StatementFormatter:
    return create_statement_formatter(version=1)


@mark.parametrize(
    "value,result",
    [  # Strings
        ("", "''"),
        ("abcd", "'abcd'"),
        ("test' OR '1' == '1", "'test'' OR ''1'' == ''1'"),
        ("test\\", "'test\\'"),
        ("don't", "'don''t'"),
        ("some\0value", "'some\\0value'"),
        # Numbers
        (1, "1"),
        (1.123, "1.123"),
        (Decimal("1.123"), "1.123"),
        (Decimal(1.123), "1.1229999999999999982236431605997495353221893310546875"),
        (True, "true"),
        (False, "false"),
        # Date, datetime
        (date(2022, 1, 10), "'2022-01-10'"),
        (datetime(2022, 1, 10, 1, 1, 1), "'2022-01-10 01:01:01'"),
        (
            datetime(2022, 1, 10, 1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            "'2022-01-10 00:01:01'",
        ),
        # List, tuple
        ([], "[]"),
        ([1, 2, 3], "[1, 2, 3]"),
        (("a", "b", "c"), "['a', 'b', 'c']"),
        # None
        (None, "NULL"),
        # Bytea
        (b"abc", "E'\\x61\\x62\\x63'"),
    ],
)
def test_format_value(formatter: StatementFormatter, value: str, result: str) -> None:
    assert formatter.format_value(value) == result, "Invalid format_value result"


@mark.parametrize(
    "value,result",
    [  # Strings
        ("", "''"),
        ("abcd", "'abcd'"),
        ("test' OR '1' == '1", "'test'' OR ''1'' == ''1'"),
        ("test\\", "'test\\\\'"),
        ("don't", "'don''t'"),
        ("some\0value", "'some\\0value'"),
        # Numbers
        (1, "1"),
        (1.123, "1.123"),
        (Decimal("1.123"), "1.123"),
        (Decimal(1.123), "1.1229999999999999982236431605997495353221893310546875"),
        (True, "true"),
        (False, "false"),
        # Date, datetime
        (date(2022, 1, 10), "'2022-01-10'"),
        (datetime(2022, 1, 10, 1, 1, 1), "'2022-01-10 01:01:01'"),
        (
            datetime(2022, 1, 10, 1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            "'2022-01-10 00:01:01'",
        ),
        # List, tuple
        ([], "[]"),
        ([1, 2, 3], "[1, 2, 3]"),
        (("a", "b", "c"), "['a', 'b', 'c']"),
        # None
        (None, "NULL"),
        # Bytea
        (b"abc", "E'\\x61\\x62\\x63'"),
    ],
)
def test_format_value_v1(
    formatter_v1: StatementFormatter, value: str, result: str
) -> None:
    assert formatter_v1.format_value(value) == result, "Invalid format_value result"


def test_format_value_errors(formatter: StatementFormatter) -> None:
    with raises(DataError) as exc_info:
        formatter.format_value(Exception())

    assert str(exc_info.value) == "unsupported parameter type <class 'Exception'>"


def to_statement(sql: str) -> Statement:
    return parse(sql)[0]


@mark.parametrize(
    "statement,params,result",
    [
        (to_statement("select * from table"), (), "select * from table"),
        (
            to_statement("select * from table where id == ?"),
            (1,),
            "select * from table where id == 1",
        ),
        (
            to_statement("select * from table where id == '?'"),
            (),
            "select * from table where id == '?'",
        ),
        (
            to_statement("insert into table values (?, ?, '?')"),
            (1, "1"),
            "insert into table values (1, '1', '?')",
        ),
        (
            to_statement("select * from t where /*comment ?*/ id == ?"),
            ("*/ 1 == 1 or /*",),
            "select * from t where /*comment ?*/ id == '*/ 1 == 1 or /*'",
        ),
        (
            to_statement("select * from t where id == ?"),
            ("' or '' == '",),
            "select * from t where id == ''' or '''' == '''",
        ),
    ],
)
def test_format_statement(
    formatter: StatementFormatter, statement: Statement, params: tuple, result: str
) -> None:
    assert (
        formatter.format_statement(statement, params) == result
    ), "Invalid format sql result"


def test_format_statement_errors(formatter: StatementFormatter) -> None:
    with raises(DataError) as exc_info:
        formatter.format_statement(to_statement("?"), [])
    assert (
        str(exc_info.value)
        == "not enough parameters provided for substitution: given 0, found one more"
    ), "Invalid not enought parameters error"

    with raises(DataError) as exc_info:
        formatter.format_statement(to_statement("?"), (1, 2))
    assert (
        str(exc_info.value)
        == "too many parameters provided for substitution: given 2, used only 1"
    ), "Invalid not enought parameters error"


@mark.parametrize(
    "query,params,result",
    [
        ("", (), [""]),
        ("select * from t", (), ["select * from t"]),
        ("select * from t;", (), ["select * from t"]),
        ("select * from t where id == ?", ((1,),), ["select * from t where id == 1"]),
        ("select * from t where id == ?;", ((1,),), ["select * from t where id == 1"]),
        (
            "select * from t;insert into t values (1, 2)",
            (),
            ["select * from t", "insert into t values (1, 2)"],
        ),
        (
            "insert into t values (1, 2);select * from t;",
            (),
            ["insert into t values (1, 2)", "select * from t"],
        ),
        (
            "select * from t where id == ?",
            ((1,), (2,)),
            ["select * from t where id == 1", "select * from t where id == 2"],
        ),
        (
            "select * from t; set a = b;",
            (),
            ["select * from t", SetParameter("a", "b")],
        ),
        (
            "set \t\na     =   \t\n b   ; set c=d;",
            (),
            [SetParameter("a", "b"), SetParameter("c", "d")],
        ),
    ],
)
def test_split_format_sql(
    formatter: StatementFormatter, query: str, params: tuple, result: List[str]
) -> None:
    assert (
        formatter.split_format_sql(query, params) == result
    ), "Invalid split and format sql result"


def test_split_format_error(formatter: StatementFormatter) -> None:
    with raises(NotSupportedError):
        formatter.split_format_sql(
            "select * from t where id == ?; insert into t values (?, ?)", ((1, 2, 3),)
        )

    with raises(NotSupportedError):
        formatter.split_format_sql("set a = ?", ((1,),))


@mark.parametrize(
    "statement,result",
    [
        (to_statement("select 1"), None),
        (to_statement("set a = b"), SetParameter("a", "b")),
        (to_statement("set a=b"), SetParameter("a", "b")),
        (to_statement("set \t\na     =   \t\n b   ;"), SetParameter("a", "b")),
        (to_statement("set /*comment*/a=b"), SetParameter("a", "b")),
        (to_statement("set a='some 'string'"), SetParameter("a", "some 'string")),
        (
            to_statement(
                'set query_parameters={"name":"param1","value":"Hello, world!"}'
            ),
            SetParameter(
                "query_parameters", '{"name":"param1","value":"Hello, world!"}'
            ),
        ),
        (to_statement("UPDATE t SET a=50 WHERE a>b"), None),
    ],
)
def test_statement_to_set(
    formatter: StatementFormatter, statement: Statement, result: Optional[SetParameter]
) -> None:
    assert (
        formatter.statement_to_set(statement) == result
    ), "Invalid statement_to_set output"


@mark.parametrize(
    "statement,error",
    [
        (to_statement("set"), InterfaceError),
        (to_statement("set a"), InterfaceError),
        (to_statement("set a ="), InterfaceError),
    ],
)
def test_statement_to_set_errors(
    formatter: StatementFormatter, statement: Statement, error: Exception
) -> None:
    with raises(error):
        formatter.statement_to_set(statement)


def test_binary() -> None:
    assert Binary("abc") == b"abc"
