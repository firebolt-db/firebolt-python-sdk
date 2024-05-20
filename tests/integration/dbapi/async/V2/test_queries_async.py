import math
from datetime import date, datetime
from decimal import Decimal
from random import randint
from typing import Callable, List

from pytest import mark, raises

from firebolt.async_db import Binary, Connection, Cursor, OperationalError
from firebolt.async_db.connection import connect
from firebolt.client.auth.base import Auth
from firebolt.common._types import ColType, Column
from tests.integration.dbapi.utils import assert_deep_eq

VALS_TO_INSERT_2 = ",".join(
    [f"({i}, {i-3}, '{val}')" for (i, val) in enumerate(range(4, 1000))]
)
LONG_INSERT = f"INSERT INTO test_tbl VALUES {VALS_TO_INSERT_2}"


async def test_connect_no_db(
    connection_no_db: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    await test_select(
        connection_no_db,
        all_types_query,
        all_types_query_description,
        all_types_query_response,
        timezone_name,
    )


async def test_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
    timezone_name: str,
) -> None:
    """Select handles all data types properly."""
    with connection.cursor() as c:
        # For timestamptz test
        assert (
            await c.execute(f"SET time_zone={timezone_name}") == -1
        ), "Invalid set statment row count"

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


async def test_select_inf(connection: Connection) -> None:
    with connection.cursor() as c:
        await c.execute("SELECT 'inf'::float, '-inf'::float")
        data = await c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"
        assert data[0][0] == float("inf"), "Invalid data returned by fetchall"
        assert data[0][1] == float("-inf"), "Invalid data returned by fetchall"


async def test_select_nan(connection: Connection) -> None:
    with connection.cursor() as c:
        await c.execute("SELECT 'nan'::float, '-nan'::float")
        data = await c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"
        assert math.isnan(data[0][0]), "Invalid data returned by fetchall"
        assert math.isnan(data[0][1]), "Invalid data returned by fetchall"


@mark.slow
@mark.timeout(timeout=550)
async def test_long_query(
    connection: Connection,
    minimal_time: Callable[[float], None],
) -> None:
    """AWS ALB TCP timeout set to 350; make sure we handle the keepalive correctly."""

    minimal_time(350)

    with connection.cursor() as c:
        await c.execute(
            "SELECT checksum(*) FROM GENERATE_SERIES(1, 400000000000)",  # approx 6m runtime
        )
        data = await c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"


async def test_drop_create(connection: Connection) -> None:
    """Create and drop table/index queries are handled properly."""

    async def test_query(c: Cursor, query: str) -> None:
        await c.execute(query)
        assert c.description == None
        assert c.rowcount == 0

    """Create table query is handled properly"""
    with connection.cursor() as c:
        # Cleanup
        await c.execute("DROP AGGREGATING INDEX IF EXISTS test_db_agg_idx")
        await c.execute("DROP TABLE IF EXISTS test_drop_create_async")
        await c.execute("DROP TABLE IF EXISTS test_drop_create_async_dim")

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

        # Create aggregating index
        await test_query(
            c,
            "CREATE AGGREGATING INDEX test_db_agg_idx ON "
            "test_drop_create_async(id, count(f), count(dt))",
        )

        # Drop aggregating index
        await test_query(c, "DROP AGGREGATING INDEX test_db_agg_idx")

        # Test drop once again
        await test_query(c, "DROP TABLE test_drop_create_async")
        await test_query(c, "DROP TABLE IF EXISTS test_drop_create_async")

        await test_query(c, "DROP TABLE test_drop_create_async_dim")
        await test_query(c, "DROP TABLE IF EXISTS test_drop_create_async_dim")


async def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled properly."""

    async def test_empty_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == 0, "Invalid row count returned"
        assert c.rowcount == 0, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        assert await c.fetchone() is None
        assert len(await c.fetchmany()) == 0
        assert len(await c.fetchall()) == 0

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
                    True,
                    [1, 2, 3],
                ],
            ],
            "Invalid data in table after insert",
        )


async def test_parameterized_query(connection: Connection) -> None:
    """Query parameters are handled properly."""

    async def test_empty_query(c: Cursor, query: str, params: tuple) -> None:
        assert await c.execute(query, params) == 0, "Invalid row count returned"
        assert c.rowcount == 0, "Invalid rowcount value"
        assert c.description is None, "Invalid description"
        assert await c.fetchone() is None
        assert len(await c.fetchmany()) == 0
        assert len(await c.fetchall()) == 0

    with connection.cursor() as c:
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
        params[2] = "text\\0"

        assert (
            await c.execute("SELECT * FROM test_tb_async_parameterized") == 1
        ), "Invalid data length in table after parameterized insert"

        assert_deep_eq(
            await c.fetchall(),
            [params + ["\\?"]],
            "Invalid data in table after parameterized insert",
        )


async def test_multi_statement_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    with connection.cursor() as c:
        await c.execute("DROP TABLE IF EXISTS test_tb_async_multi_statement")
        await c.execute(
            "CREATE FACT TABLE test_tb_async_multi_statement(i int, s string)"
            " primary index i"
        )

        await c.execute(
            "INSERT INTO test_tb_async_multi_statement values (1, 'a'), (2, 'b');"
            "SELECT * FROM test_tb_async_multi_statement;"
            "SELECT * FROM test_tb_async_multi_statement WHERE i <= 1"
        )
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


async def test_set_invalid_parameter(connection: Connection):
    with connection.cursor() as c:
        assert len(c._set_parameters) == 0
        with raises(OperationalError):
            await c.execute("SET some_invalid_parameter = 1")

        assert len(c._set_parameters) == 0


async def test_bytea_roundtrip(
    connection: Connection,
) -> None:
    """Inserted and than selected bytea value doesn't get corrupted."""
    with connection.cursor() as c:
        await c.execute("DROP TABLE IF EXISTS test_bytea_roundtrip_2")
        await c.execute(
            "CREATE FACT TABLE test_bytea_roundtrip_2(id int, b bytea) primary index id"
        )

        data = "bytea_123\n\tヽ༼ຈل͜ຈ༽ﾉ"

        await c.execute(
            "INSERT INTO test_bytea_roundtrip_2 VALUES (1, ?)", (Binary(data),)
        )
        await c.execute("SELECT b FROM test_bytea_roundtrip_2")

        bytes_data = (await c.fetchone())[0]

        assert (
            bytes_data.decode("utf-8") == data
        ), "Invalid bytea data returned after roundtrip"


@mark.account_v2
async def test_account_v2_connection_with_db(
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> None:
    async with await connect(
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        # This fails if we're not running with a db context
        await connection.cursor().execute(
            "SELECT * FROM information_schema.tables LIMIT 1"
        )


@mark.account_v2
async def test_account_v2_connection_with_db_and_engine(
    database_name: str,
    connection_system_engine: Connection,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    engine_name: str,
) -> None:
    system_cursor = connection_system_engine.cursor()
    # We can only connect to a running engine so start it first
    # via the system connection to keep test isolated
    await system_cursor.execute(f"START ENGINE {engine_name}")
    async with await connect(
        database=database_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        # generate a random string to avoid name conflicts
        rnd_suffix = str(randint(0, 1000))
        cursor = connection.cursor()
        await cursor.execute(f"CREATE TABLE test_table_{rnd_suffix} (id int)")
        # This fails if we're not running on a user engine
        await cursor.execute(f"INSERT INTO test_table_{rnd_suffix} VALUES (1)")
