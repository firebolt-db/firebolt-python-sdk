from datetime import date, datetime
from typing import Any, List

from pytest import mark

from firebolt.async_db import Connection, Cursor
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
async def test_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
) -> None:
    """Select handles all data types properly"""
    with connection.cursor() as c:
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
async def test_drop_create(
    connection: Connection, create_drop_description: List[Column]
) -> None:
    """Create and drop table/index queries are handled propperly"""

    async def test_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        assert_deep_eq(
            c.description,
            create_drop_description,
            "Invalid create table query description",
        )
        assert len(await c.fetchall()) == 1, "Invalid data returned"

    """Create table query is handled properly"""
    with connection.cursor() as c:
        # Cleanup
        await c.execute("DROP JOIN INDEX IF EXISTS test_db_join_idx")
        await c.execute("DROP AGGREGATING INDEX IF EXISTS test_db_agg_idx")
        await c.execute("DROP TABLE IF EXISTS test_tb")
        await c.execute("DROP TABLE IF EXISTS test_tb_dim")

        # Fact table
        await test_query(
            c,
            "CREATE FACT TABLE test_tb(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id",
        )

        # Dimension table
        await test_query(
            c,
            "CREATE DIMENSION TABLE test_tb_dim(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int))",
        )

        # Create join index
        await test_query(
            c, "CREATE JOIN INDEX test_db_join_idx ON test_tb_dim(id, sn, f)"
        )

        # Create aggregating index
        await test_query(
            c,
            "CREATE AGGREGATING INDEX test_db_agg_idx ON "
            "test_tb(id, sum(f), count(dt))",
        )

        # Drop join index
        await test_query(c, "DROP JOIN INDEX test_db_join_idx")

        # Drop aggregating index
        await test_query(c, "DROP AGGREGATING INDEX test_db_agg_idx")

        # Test drop once again
        await test_query(c, "DROP TABLE test_tb")
        await test_query(c, "DROP TABLE IF EXISTS test_tb")

        await test_query(c, "DROP TABLE test_tb_dim")
        await test_query(c, "DROP TABLE IF EXISTS test_tb_dim")


@mark.asyncio
async def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled propperly"""

    async def test_empty_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == -1, "Invalid row count returned"
        assert c.rowcount == -1, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        assert await c.fetchone() is None, "Invalid data returned"

    with connection.cursor() as c:
        await c.execute("DROP TABLE IF EXISTS test_tb")
        await c.execute(
            "CREATE FACT TABLE test_tb(id int, sn string null, f float,"
            "d date, dt datetime, b bool, a array(int)) primary index id"
        )

        await test_empty_query(
            c,
            "INSERT INTO test_tb VALUES (1, 'sn', 1.1, '2021-01-01',"
            "'2021-01-01 01:01:01', true, [1, 2, 3])",
        )

        await test_empty_query(
            c,
            "INSERT INTO test_tb VALUES (2, null, 2.2, '2022-02-02',"
            "'2022-02-02 02:02:02', false, [1])",
        )

        assert (
            await c.execute("SELECT * FROM test_tb ORDER BY test_tb.id") == 2
        ), "Invalid data length in table after insert"

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
                [2, None, 2.2, date(2022, 2, 2), datetime(2022, 2, 2, 2, 2, 2), 0, [1]],
            ],
            "Invalid data in table after insert",
        )
