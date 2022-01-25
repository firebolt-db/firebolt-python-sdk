from datetime import date, datetime
from typing import Any, List

from pytest import raises

from firebolt.async_db._types import ColType
from firebolt.async_db.cursor import Column
from firebolt.db import Connection, Cursor, DataError


def assert_deep_eq(got: Any, expected: Any, msg: str) -> bool:
    if type(got) == list and type(expected) == list:
        all([assert_deep_eq(f, s, msg) for f, s in zip(got, expected)])
    assert (
        type(got) == type(expected) and got == expected
    ), f"{msg}: {got}(got) != {expected}(expected)"


def test_connect_engine_name(
    connection_engine_name: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Connecting with engine name is handled properly."""
    test_select(
        connection_engine_name,
        all_types_query,
        all_types_query_description,
        all_types_query_response,
    )


def test_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Select handles all data types properly"""
    with connection.cursor() as c:
        assert c.execute(all_types_query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        data = c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(c.fetchall()) == 0, "Redundant data returned by fetchall"

        # Different fetch types
        c.execute(all_types_query)
        assert c.fetchone() == all_types_query_response[0], "Invalid fetchone data"
        assert c.fetchone() is None, "Redundant data returned by fetchone"

        c.execute(all_types_query)
        assert len(c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data, all_types_query_response, "Invalid data returned by fetchmany"
        )


def test_drop_create(
    connection: Connection, create_drop_description: List[Column]
) -> None:
    """Create and drop table/index queries are handled properly."""

    def test_query(c: Cursor, query: str) -> None:
        assert c.execute(query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        assert_deep_eq(
            c.description,
            create_drop_description,
            "Invalid create table query description",
        )
        assert len(c.fetchall()) == 1, "Invalid data returned"

    """Create table query is handled properly"""
    with connection.cursor() as c:
        # Cleanup
        c.execute("DROP JOIN INDEX IF EXISTS test_db_join_idx")
        c.execute("DROP AGGREGATING INDEX IF EXISTS test_db_agg_idx")
        c.execute("DROP TABLE IF EXISTS test_tb")
        c.execute("DROP TABLE IF EXISTS test_tb_dim")

        # Fact table
        test_query(
            c,
            "CREATE FACT TABLE test_tb(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id",
        )

        # Dimension table
        test_query(
            c,
            "CREATE DIMENSION TABLE test_tb_dim(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int))",
        )

        # Create join index
        test_query(c, "CREATE JOIN INDEX test_db_join_idx ON test_tb_dim(id, sn, f)")

        # Create aggregating index
        test_query(
            c,
            "CREATE AGGREGATING INDEX test_db_agg_idx ON "
            "test_tb(id, sum(f), count(dt))",
        )

        # Drop join index
        test_query(c, "DROP JOIN INDEX test_db_join_idx")

        # Drop aggregating index
        test_query(c, "DROP AGGREGATING INDEX test_db_agg_idx")

        # Test drop once again
        test_query(c, "DROP TABLE test_tb")
        test_query(c, "DROP TABLE IF EXISTS test_tb")

        test_query(c, "DROP TABLE test_tb_dim")
        test_query(c, "DROP TABLE IF EXISTS test_tb_dim")


def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled properly."""

    def test_empty_query(c: Cursor, query: str) -> None:
        assert c.execute(query) == -1, "Invalid row count returned"
        assert c.rowcount == -1, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        with raises(DataError):
            c.fetchone()

        with raises(DataError):
            c.fetchmany()

        with raises(DataError):
            c.fetchall()

    with connection.cursor() as c:
        c.execute("DROP TABLE IF EXISTS test_tb")
        c.execute(
            "CREATE FACT TABLE test_tb(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id"
        )

        test_empty_query(
            c,
            "INSERT INTO test_tb VALUES (1, 'sn', 1.1, '2021-01-01',"
            "'2021-01-01 01:01:01', true, [1, 2, 3])",
        )

        assert (
            c.execute("SELECT * FROM test_tb ORDER BY test_tb.id") == 1
        ), "Invalid data length in table after insert"

        assert_deep_eq(
            c.fetchall(),
            [
                [
                    1,
                    "sn",
                    1.1,
                    date(2021, 1, 1),
                    datetime(2021, 1, 1, 1, 1, 1),
                    1,
                    [1, 2, 3],
                ],
            ],
            "Invalid data in table after insert",
        )


def test_parameterized_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    def test_empty_query(c: Cursor, query: str, params: tuple) -> None:
        assert c.execute(query, params) == -1, "Invalid row count returned"
        assert c.rowcount == -1, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        with raises(DataError):
            c.fetchone()

        with raises(DataError):
            c.fetchmany()

        with raises(DataError):
            c.fetchall()

    with connection.cursor() as c:
        c.execute("DROP TABLE IF EXISTS test_tb_parameterized")
        c.execute(
            "CREATE FACT TABLE test_tb_parameterized(i int, f float, s string, sn"
            " string null, d date, dt datetime, b bool, a array(int), ss string)"
            " primary index i"
        )

        params = [
            1,
            1.123,
            "text\0",
            None,
            date(2022, 1, 1),
            datetime(2022, 1, 1, 1, 1, 1),
            True,
            [1, 2, 3],
        ]

        test_empty_query(
            c,
            "INSERT INTO test_tb_parameterized VALUES (?, ?, ?, ?, ?, ?, ?, ?, '\\?')",
            params,
        )

        # \0 is converted to 0
        params[2] = "text0"

        # Bool is converted to int
        params[6] = 1

        assert (
            c.execute("SELECT * FROM test_tb_parameterized") == 1
        ), "Invalid data length in table after parameterized insert"

        assert_deep_eq(
            c.fetchall(),
            [params + ["?"]],
            "Invalid data in table after parameterized insert",
        )


def test_multi_statement_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    with connection.cursor() as c:
        c.execute("DROP TABLE IF EXISTS test_tb_multi_statement")
        c.execute(
            "CREATE FACT TABLE test_tb_multi_statement(i int, s string) primary index i"
        )

        assert (
            c.execute(
                "INSERT INTO test_tb_multi_statement values (1, 'a'), (2, 'b');"
                "SELECT * FROM test_tb_multi_statement"
            )
            == -1
        ), "Invalid row count returned for insert"
        assert c.rowcount == -1, "Invalid row count"
        assert c.description is None, "Invalid description"

        assert c.nextset()

        assert c.rowcount == 2, "Invalid select row count"
        assert_deep_eq(
            c.description,
            [
                Column("i", int, None, None, None, None, None),
                Column("s", str, None, None, None, None, None),
            ],
            "Invalid select query description",
        )

        assert_deep_eq(
            c.fetchall(),
            [[1, "a"], [2, "b"]],
            "Invalid data in table after parameterized insert",
        )

        assert c.nextset() is None
