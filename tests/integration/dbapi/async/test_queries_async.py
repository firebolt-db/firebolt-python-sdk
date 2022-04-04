from datetime import date, datetime
from decimal import Decimal
from typing import Any, List

from pytest import mark, raises

from firebolt.async_db import Connection, Cursor, DataError, OperationalError
from firebolt.async_db._types import ColType, Column


def assert_deep_eq(got: Any, expected: Any, msg: str) -> bool:
    if type(got) == list and type(expected) == list:
        all([assert_deep_eq(f, s, msg) for f, s in zip(got, expected)])
    assert (
        type(got) == type(expected) and got == expected
    ), f"{msg}: {got}(got) != {expected}(expected)"


@mark.asyncio
async def test_connect_engine_name(
    connection_engine_name: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Connecting with engine name is handled properly."""
    await test_select(
        connection_engine_name,
        all_types_query,
        all_types_query_description,
        all_types_query_response,
    )


@mark.asyncio
async def test_connect_no_engine(
    connection_no_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Connecting with engine name is handled properly."""
    await test_select(
        connection_no_engine,
        all_types_query,
        all_types_query_description,
        all_types_query_response,
    )


@mark.asyncio
async def test_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Select handles all data types properly"""
    with connection.cursor() as c:
        assert (await c.execute("set firebolt_use_decimal = 1")) == -1
        assert await c.execute(all_types_query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        data = await c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(await c.fetchall()) == 0, "Redundant data returned by fetchall"

        # Different fetch types
        await c.execute(all_types_query)
        assert (
            await c.fetchone() == all_types_query_response[0]
        ), "Invalid fetchone data"
        assert await c.fetchone() is None, "Redundant data returned by fetchone"

        await c.execute(all_types_query)
        assert len(await c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = await c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data, all_types_query_response, "Invalid data returned by fetchmany"
        )


@mark.asyncio
@mark.timeout(timeout=400)
async def test_long_query(
    connection: Connection,
) -> None:
    """AWS ALB TCP timeout set to 350, make sure we handle the keepalive correctly"""
    with connection.cursor() as c:
        await c.execute(
            "SET advanced_mode = 1; SET use_standard_sql = 0;"
            "SELECT sleepEachRow(1) from numbers(360)",
        )
        await c.nextset()
        await c.nextset()
        data = await c.fetchall()
        assert len(data) == 360, "Invalid data size returned by fetchall"


@mark.asyncio
async def test_drop_create(
    connection: Connection, create_drop_description: List[Column]
) -> None:
    """Create and drop table/index queries are handled properly."""

    async def test_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == 1, "Invalid row count returned."
        assert c.rowcount == 1, "Invalid rowcount value."
        assert_deep_eq(
            c.description,
            create_drop_description,
            "Invalid create table query description.",
        )
        assert len(await c.fetchall()) == 1, "Invalid data returned."

    """Create table query is handled properly"""
    with connection.cursor() as c:
        # Cleanup
        await c.execute("DROP JOIN INDEX IF EXISTS test_drop_create_async_db_join_idx")
        await c.execute(
            "DROP AGGREGATING INDEX IF EXISTS test_drop_create_async_db_agg_idx"
        )
        await c.execute("DROP TABLE IF EXISTS test_drop_create_async_tb")
        await c.execute("DROP TABLE IF EXISTS test_drop_create_async_tb_dim")

        # Fact table
        await test_query(
            c,
            "CREATE FACT TABLE test_drop_create_async(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id",
        )

        # Dimension table
        await test_query(
            c,
            "CREATE DIMENSION TABLE test_drop_create_async_dim(id int, sn string null"
            ", f float, d date, dt datetime, b bool, a array(int))",
        )

        # Create join index
        await test_query(
            c,
            "CREATE JOIN INDEX test_db_join_idx ON "
            "test_drop_create_async_dim(id, sn, f)",
        )

        # Create aggregating index
        await test_query(
            c,
            "CREATE AGGREGATING INDEX test_db_agg_idx ON "
            "test_drop_create_async(id, sum(f), count(dt))",
        )

        # Drop join index
        await test_query(c, "DROP JOIN INDEX test_db_join_idx")

        # Drop aggregating index
        await test_query(c, "DROP AGGREGATING INDEX test_db_agg_idx")

        # Test drop once again
        await test_query(c, "DROP TABLE test_drop_create_async")
        await test_query(c, "DROP TABLE IF EXISTS test_drop_create_async")

        await test_query(c, "DROP TABLE test_drop_create_async_dim")
        await test_query(c, "DROP TABLE IF EXISTS test_drop_create_async_dim")


@mark.asyncio
async def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled properly."""

    async def test_empty_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == -1, "Invalid row count returned."
        assert c.rowcount == -1, "Invalid rowcount value."
        assert c.description is None, "Invalid description."
        with raises(DataError):
            await c.fetchone()

        with raises(DataError):
            await c.fetchmany()

        with raises(DataError):
            await c.fetchall()

    with connection.cursor() as c:
        await c.execute("DROP TABLE IF EXISTS test_insert_async_tb")
        await c.execute(
            "CREATE FACT TABLE test_insert_async_tb(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id"
        )

        await test_empty_query(
            c,
            "INSERT INTO test_insert_async_tb VALUES (1, 'sn', 1.1, '2021-01-01',"
            "'2021-01-01 01:01:01', true, [1, 2, 3])",
        )

        assert (
            await c.execute(
                "SELECT * FROM test_insert_async_tb ORDER BY test_insert_async_tb.id"
            )
            == 1
        ), "Invalid data length in table after insert."

        assert_deep_eq(
            await c.fetchall(),
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
            "Invalid data in table after insert.",
        )


@mark.asyncio
async def test_parameterized_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    async def test_empty_query(c: Cursor, query: str, params: tuple) -> None:
        assert await c.execute(query, params) == -1, "Invalid row count returned"
        assert c.rowcount == -1, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        with raises(DataError):
            await c.fetchone()

        with raises(DataError):
            await c.fetchmany()

        with raises(DataError):
            await c.fetchall()

    with connection.cursor() as c:
        await c.execute("set firebolt_use_decimal = 1")
        await c.execute("DROP TABLE IF EXISTS test_tb_async_parameterized")
        await c.execute(
            "CREATE FACT TABLE test_tb_async_parameterized(i int, f float, s string, sn"
            " string null, d date, dt datetime, b bool, a array(int), "
            "dec decimal(38, 3), ss string) primary index i",
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
            Decimal("123.456"),
        ]

        await test_empty_query(
            c,
            "INSERT INTO test_tb_async_parameterized VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, '\\?')",
            params,
        )

        # \0 is converted to 0
        params[2] = "text0"

        # Bool is converted to int
        params[6] = 1

        assert (
            await c.execute("SELECT * FROM test_tb_async_parameterized") == 1
        ), "Invalid data length in table after parameterized insert"

        assert_deep_eq(
            await c.fetchall(),
            [params + ["?"]],
            "Invalid data in table after parameterized insert",
        )


@mark.asyncio
async def test_multi_statement_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    with connection.cursor() as c:
        await c.execute("DROP TABLE IF EXISTS test_tb_async_multi_statement")
        await c.execute(
            "CREATE FACT TABLE test_tb_async_multi_statement(i int, s string)"
            " primary index i"
        )

        assert (
            await c.execute(
                "INSERT INTO test_tb_async_multi_statement values (1, 'a'), (2, 'b');"
                "SELECT * FROM test_tb_async_multi_statement;"
                "SELECT * FROM test_tb_async_multi_statement WHERE i <= 1"
            )
            == -1
        ), "Invalid row count returned for insert"
        assert c.rowcount == -1, "Invalid row count"
        assert c.description is None, "Invalid description"

        assert await c.nextset()

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
            await c.fetchall(),
            [[1, "a"], [2, "b"]],
            "Invalid data in table after parameterized insert",
        )

        assert await c.nextset()

        assert c.rowcount == 1, "Invalid select row count"
        assert_deep_eq(
            c.description,
            [
                Column("i", int, None, None, None, None, None),
                Column("s", str, None, None, None, None, None),
            ],
            "Invalid select query description",
        )

        assert_deep_eq(
            await c.fetchall(),
            [[1, "a"]],
            "Invalid data in table after parameterized insert",
        )

        assert await c.nextset() is None


@mark.asyncio
async def test_set_invalid_parameter(connection: Connection):
    with connection.cursor() as c:
        assert len(c._set_parameters) == 0
        with raises(OperationalError):
            await c.execute("set some_invalid_parameter = 1")

        assert len(c._set_parameters) == 0
