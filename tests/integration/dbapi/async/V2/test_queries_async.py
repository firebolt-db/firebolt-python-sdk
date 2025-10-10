import math
from datetime import date, datetime
from decimal import Decimal
from random import randint
from typing import Callable, List, Tuple

from pytest import mark, raises

import firebolt.async_db
from firebolt.async_db import Binary, Connection, Cursor, OperationalError
from firebolt.async_db.connection import connect
from firebolt.client.auth.base import Auth
from firebolt.common._types import ColType
from firebolt.common.row_set.types import Column
from firebolt.utils.exception import FireboltStructuredError
from tests.integration.dbapi.conftest import LONG_SELECT_DEFAULT_V2
from tests.integration.dbapi.utils import assert_deep_eq

VALS_TO_INSERT_2 = ",".join(
    [f"({i}, {i-3}, '{val}')" for (i, val) in enumerate(range(4, 1000))]
)
LONG_INSERT = f'INSERT INTO "test_tbl" VALUES {VALS_TO_INSERT_2}'
LONG_SELECT = (
    "SELECT checksum(*) FROM GENERATE_SERIES(1, {long_value})"  # approx 6m runtime
)


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
    async with connection.cursor() as c:
        # For timestamptz test
        assert (
            await c.execute(f"SET timezone={timezone_name}") == -1
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
    async with connection.cursor() as c:
        await c.execute("SELECT 'inf'::float, '-inf'::float")
        data = await c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"
        assert data[0][0] == float("inf"), "Invalid data returned by fetchall"
        assert data[0][1] == float("-inf"), "Invalid data returned by fetchall"


async def test_select_nan(connection: Connection) -> None:
    async with connection.cursor() as c:
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
    long_test_value: Callable[[int], int],
) -> None:
    """AWS ALB TCP timeout set to 350; make sure we handle the keepalive correctly."""

    minimal_time(350)

    async with connection.cursor() as c:
        await c.execute(
            LONG_SELECT.format(long_value=long_test_value(LONG_SELECT_DEFAULT_V2))
        )
        data = await c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"


# Not compatible with core
@mark.parametrize("connection", ["remote"], indirect=True)
async def test_drop_create(connection: Connection) -> None:
    """Create and drop table/index queries are handled properly."""

    async def test_query(c: Cursor, query: str) -> None:
        await c.execute(query)
        assert c.description == []
        assert c.rowcount == 0

    """Create table query is handled properly"""
    async with connection.cursor() as c:
        # Cleanup
        await c.execute('DROP AGGREGATING INDEX IF EXISTS "test_db_agg_idx"')
        await c.execute('DROP TABLE IF EXISTS "test_drop_create_async"')
        await c.execute('DROP TABLE IF EXISTS "test_drop_create_async_dim"')

        # Fact table
        await test_query(
            c,
            'CREATE FACT TABLE "test_drop_create_async"(id int, sn string null, f float,'
            "d date, dt datetime, b bool, a array(int)) primary index id",
        )

        # Dimension table
        await test_query(
            c,
            'CREATE DIMENSION TABLE "test_drop_create_async_dim"(id int, sn string null'
            ", f float, d date, dt datetime, b bool, a array(int))",
        )

        # Create aggregating index
        await test_query(
            c,
            'CREATE AGGREGATING INDEX "test_db_agg_idx" ON '
            "test_drop_create_async(id, count(f), count(dt))",
        )

        # Drop aggregating index
        await test_query(c, 'DROP AGGREGATING INDEX "test_db_agg_idx"')

        # Test drop once again
        await test_query(c, 'DROP TABLE "test_drop_create_async"')
        await test_query(c, 'DROP TABLE IF EXISTS "test_drop_create_async"')

        await test_query(c, 'DROP TABLE "test_drop_create_async_dim"')
        await test_query(c, 'DROP TABLE IF EXISTS "test_drop_create_async_dim"')


async def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled properly."""

    async def test_empty_query(c: Cursor, query: str) -> None:
        assert await c.execute(query) == 0, "Invalid row count returned"
        assert c.rowcount == 0, "Invalid rowcount value"
        assert c.description == [], "Invalid description"
        assert await c.fetchone() is None
        assert len(await c.fetchmany()) == 0
        assert len(await c.fetchall()) == 0

    async with connection.cursor() as c:
        await c.execute('DROP TABLE IF EXISTS "test_insert_async_tb"')
        await c.execute(
            'CREATE FACT TABLE "test_insert_async_tb"(id int, sn string null, f float,'
            "d date, dt datetime, b bool, a array(int)) primary index id"
        )

        await test_empty_query(
            c,
            "INSERT INTO \"test_insert_async_tb\" VALUES (1, 'sn', 1.1, '2021-01-01',"
            "'2021-01-01 01:01:01', true, [1, 2, 3])",
        )

        assert (
            await c.execute(
                'SELECT * FROM "test_insert_async_tb" ORDER BY "test_insert_async_tb".id'
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
        assert c.description == [], "Invalid description"
        assert await c.fetchone() is None
        assert len(await c.fetchmany()) == 0
        assert len(await c.fetchall()) == 0

    async with connection.cursor() as c:
        await c.execute('DROP TABLE IF EXISTS "test_tb_async_parameterized"')
        await c.execute(
            'CREATE FACT TABLE "test_tb_async_parameterized"(i int, f float, s string, sn'
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
            'INSERT INTO "test_tb_async_parameterized" VALUES '
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, '\\?')",
            params,
        )

        # \0 is converted to 0
        params[2] = "text\\0"

        assert (
            await c.execute('SELECT * FROM "test_tb_async_parameterized"') == 1
        ), "Invalid data length in table after parameterized insert"

        assert_deep_eq(
            await c.fetchall(),
            [params + ["\\?"]],
            "Invalid data in table after parameterized insert",
        )


async def test_parameterized_query_with_special_chars(connection: Connection) -> None:
    """Query parameters are handled properly."""
    async with connection.cursor() as c:
        parameters = ["text with 'quote'", "text with \\slashes"]

        await c.execute(
            "SELECT ? as one, ? as two",
            parameters,
        )

        result = await c.fetchall()
        assert result == [
            [parameters[0], parameters[1]]
        ], "Invalid data in table after parameterized insert"


@mark.parametrize("paramstyle", ["qmark", "fb_numeric"])
async def test_executemany_bulk_insert(connection: Connection, paramstyle: str) -> None:
    """executemany with bulk_insert=True inserts data correctly."""
    original_paramstyle = firebolt.async_db.paramstyle

    try:
        firebolt.async_db.paramstyle = paramstyle

        async with connection.cursor() as c:
            await c.execute('DROP TABLE IF EXISTS "test_bulk_insert_async"')
            await c.execute(
                'CREATE FACT TABLE "test_bulk_insert_async"(id int, name string) primary index id'
            )

            if paramstyle == "qmark":
                await c.executemany(
                    'INSERT INTO "test_bulk_insert_async" VALUES (?, ?)',
                    [(1, "alice"), (2, "bob"), (3, "charlie")],
                    bulk_insert=True,
                )
            else:
                await c.executemany(
                    'INSERT INTO "test_bulk_insert_async" VALUES ($1, $2)',
                    [(1, "alice"), (2, "bob"), (3, "charlie")],
                    bulk_insert=True,
                )

            await c.execute('SELECT * FROM "test_bulk_insert_async" ORDER BY id')
            data = await c.fetchall()
            assert len(data) == 3
            assert data[0] == [1, "alice"]
            assert data[1] == [2, "bob"]
            assert data[2] == [3, "charlie"]

            await c.execute('DROP TABLE "test_bulk_insert_async"')
    finally:
        firebolt.async_db.paramstyle = original_paramstyle


async def test_multi_statement_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    async with connection.cursor() as c:
        await c.execute('DROP TABLE IF EXISTS "test_tb_async_multi_statement"')
        await c.execute(
            'CREATE FACT TABLE "test_tb_async_multi_statement"(i int, s string)'
            " primary index i"
        )

        await c.execute(
            "INSERT INTO \"test_tb_async_multi_statement\" values (1, 'a'), (2, 'b');"
            'SELECT * FROM "test_tb_async_multi_statement";'
            'SELECT * FROM "test_tb_async_multi_statement" WHERE i <= 1'
        )
        assert c.description == [], "Invalid description"

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

        assert await c.nextset() is False


async def test_set_invalid_parameter(connection: Connection):
    async with connection.cursor() as c:
        assert len(c._set_parameters) == 0
        with raises((OperationalError, FireboltStructuredError)) as e:
            await c.execute("SET some_invalid_parameter = 1")

        assert "Unknown setting" in str(e.value) or "query param not allowed" in str(
            e.value
        ), "Invalid error message"
        assert len(c._set_parameters) == 0


async def test_bytea_roundtrip(
    connection: Connection,
) -> None:
    """Inserted and than selected bytea value doesn't get corrupted."""
    async with connection.cursor() as c:
        await c.execute('DROP TABLE IF EXISTS "test_bytea_roundtrip_2"')
        await c.execute(
            'CREATE FACT TABLE "test_bytea_roundtrip_2"(id int, b bytea) primary index id'
        )

        data = "bytea_123\n\tヽ༼ຈل͜ຈ༽ﾉ"

        await c.execute(
            'INSERT INTO "test_bytea_roundtrip_2" VALUES (1, ?)', (Binary(data),)
        )
        await c.execute('SELECT b FROM "test_bytea_roundtrip_2"')

        bytes_data = (await c.fetchone())[0]

        assert (
            bytes_data.decode("utf-8") == data
        ), "Invalid bytea data returned after roundtrip"


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
        await cursor.execute(f'CREATE TABLE "test_table_{rnd_suffix}" (id int)')
        # This fails if we're not running on a user engine
        await cursor.execute(f'INSERT INTO "test_table_{rnd_suffix}" VALUES (1)')


async def test_connection_with_mixed_case_db_and_engine(
    mixed_case_db_and_engine: Tuple[str, str],
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> None:
    test_db_name, test_engine_name = mixed_case_db_and_engine
    async with await connect(
        account_name=account_name,
        api_endpoint=api_endpoint,
        auth=auth,
        database=test_db_name,
        engine_name=test_engine_name,
    ) as connection:
        cursor = connection.cursor()
        await cursor.execute('CREATE TABLE IF NOT EXISTS "test_table" (id int)')
        # This fails if we're not running on a user engine
        await cursor.execute('INSERT INTO "test_table" VALUES (1)')


async def test_select_geography(
    connection: Connection,
    select_geography_query: str,
    select_geography_description: List[Column],
    select_geography_response: List[ColType],
) -> None:
    async with connection.cursor() as c:
        await c.execute(select_geography_query)
        assert (
            c.description == select_geography_description
        ), "Invalid description value"
        res = await c.fetchall()
        assert len(res) == 1, "Invalid data length"
        assert_deep_eq(
            res,
            select_geography_response,
            "Invalid data returned by fetchall",
        )


async def test_select_struct(
    connection: Connection,
    setup_struct_query: str,
    cleanup_struct_query: str,
    select_struct_query: str,
    select_struct_description: List[Column],
    select_struct_response: List[ColType],
):
    async with connection.cursor() as c:
        try:
            await c.execute(setup_struct_query)
            await c.execute(select_struct_query)
            assert (
                c.description == select_struct_description
            ), "Invalid description value"
            res = await c.fetchall()
            assert len(res) == 1, "Invalid data length"
            assert_deep_eq(
                res,
                select_struct_response,
                "Invalid data returned by fetchall",
            )
        finally:
            await c.execute(cleanup_struct_query)


async def test_fb_numeric_paramstyle_all_types(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: insert/select all supported types, and parameter count errors."""
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        async with connection.cursor() as c:
            await c.execute('DROP TABLE IF EXISTS "test_fb_numeric_all_types"')
            await c.execute(
                'CREATE FACT TABLE "test_fb_numeric_all_types" ('
                "i INT, f FLOAT, s STRING, sn STRING NULL, d DATE, dt DATETIME, b BOOL, "
                "a_int ARRAY(INT), dec DECIMAL(38, 3), "
                "a_str ARRAY(STRING), a_nested ARRAY(ARRAY(INT)), "
                "by BYTEA"
                ")"
            )
            params = [
                1,  # i INT
                1.123,  # f FLOAT
                "text",  # s STRING
                None,  # sn STRING NULL
                date(2022, 1, 1),  # d DATE
                datetime(2022, 1, 1, 1, 1, 1),  # dt DATETIME
                True,  # b BOOL
                [1, 2, 3],  # a_int ARRAY(INT)
                Decimal("123.456"),  # dec DECIMAL(38, 3)
                ["hello", "world", "test"],  # a_str ARRAY(STRING)
                [[1, 2], [3, 4], [5]],  # a_nested ARRAY(ARRAY(INT))
                Binary("test_bytea_data"),  # by BYTEA
            ]
            await c.execute(
                'INSERT INTO "test_fb_numeric_all_types" VALUES '
                "($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                params,
            )
            await c.execute(
                'SELECT * FROM "test_fb_numeric_all_types" WHERE i = $1', [1]
            )
            result = await c.fetchall()
            # None is returned as None, arrays as lists, decimals as Decimal, bytea as bytes
            assert result == [params]


async def test_fb_numeric_paramstyle_not_enough_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: not enough parameters supplied."""
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        async with connection.cursor() as c:
            with raises(FireboltStructuredError) as exc_info:
                await c.execute("SELECT $1, $2", [1])
            assert (
                "query referenced positional parameter $2, but it was not set"
                in str(exc_info.value).lower()
            )


async def test_fb_numeric_paramstyle_too_many_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: too many parameters supplied (should succeed)."""
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        async with connection.cursor() as c:
            await c.execute('DROP TABLE IF EXISTS "test_fb_numeric_params2"')
            await c.execute(
                'CREATE FACT TABLE "test_fb_numeric_params2" (i INT, s STRING)'
            )
            # Three params for two placeholders: should succeed, extra param ignored
            await c.execute(
                'INSERT INTO "test_fb_numeric_params2" VALUES ($1, $2)',
                [1, "foo", 123],
            )
            await c.execute('SELECT * FROM "test_fb_numeric_params2" WHERE i = $1', [1])
            result = await c.fetchall()
            assert result == [[1, "foo"]]


async def test_fb_numeric_paramstyle_incorrect_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        c = connection.cursor()
        with raises(FireboltStructuredError) as exc_info:
            await c.execute(
                "SELECT $34, $72",
                [1, "foo"],
            )
        assert "Query referenced positional parameter $34, but it was not set" in str(
            exc_info.value
        )


async def test_engine_switch(
    database_name: str,
    connection_system_engine: Connection,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    engine_name: str,
) -> None:
    system_cursor = connection_system_engine.cursor()
    await system_cursor.execute("SELECT current_engine()")
    result = await system_cursor.fetchone()
    assert (
        result[0] == "system"
    ), f"Incorrect setup - system engine cursor points at {result[0]}"
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        await cursor.execute("SELECT current_engine()")
        result = await cursor.fetchone()
        assert result[0] == engine_name, "Engine switch failed"
        # Test switching back to system engine
        await cursor.execute("USE ENGINE system")
        await cursor.execute("SELECT current_engine()")
        result = await cursor.fetchone()
        assert result[0] == "system", "Switching back to system engine failed"


async def test_database_switch(
    database_name: str,
    connection_system_engine_no_db: Connection,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    engine_name: str,
) -> None:
    system_cursor = connection_system_engine_no_db.cursor()
    await system_cursor.execute("SELECT current_database()")
    result = await system_cursor.fetchone()
    assert (
        result[0] == "account_db"
    ), f"Incorrect setup - system engine cursor points at {result[0]}"
    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        await cursor.execute("SELECT current_database()")
        result = await cursor.fetchone()
        assert result[0] == database_name, "Database switch failed"
        try:
            # Test switching back to system database
            await system_cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {database_name}_switch"
            )
            await cursor.execute(f"USE DATABASE {database_name}_switch")
            await cursor.execute("SELECT current_database()")
            result = await cursor.fetchone()
            assert (
                result[0] == f"{database_name}_switch"
            ), "Switching back to switch database failed"
        finally:
            await system_cursor.execute(
                f"DROP DATABASE IF EXISTS {database_name}_switch"
            )


async def test_select_quoted_decimal(
    connection: Connection, long_decimal_value: str, long_value_decimal_sql: str
):
    async with connection.cursor() as c:
        await c.execute(long_value_decimal_sql)
        result = await c.fetchall()
        assert len(result) == 1, "Invalid data length returned by fetchall"
        assert result[0][0] == Decimal(
            long_decimal_value
        ), "Invalid data returned by fetchall"


async def test_select_quoted_bigint(
    connection: Connection, long_bigint_value: str, long_value_bigint_sql: str
):
    async with connection.cursor() as c:
        await c.execute(long_value_bigint_sql)
        result = await c.fetchall()
        assert len(result) == 1, "Invalid data length returned by fetchall"
        assert result[0][0] == int(
            long_bigint_value
        ), "Invalid data returned by fetchall"
