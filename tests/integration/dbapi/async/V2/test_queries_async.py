import math
from datetime import date, datetime
from decimal import Decimal
from random import randint
from typing import Any, Callable, List, Tuple

import trio
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


async def check_data_visibility_async(
    table_name: str,
    row_id: int,
    connection_factory: Callable[..., Connection],
    expected_visible: bool,
    expected_data: Any = None,
) -> None:
    """Check if data is visible using a separate autocommit connection."""
    async with await connection_factory() as check_connection:
        cursor = check_connection.cursor()
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = {row_id}')
        data = await cursor.fetchall()

        if expected_visible:
            assert len(data) == 1, f"Data should be visible for id={row_id}"
            if expected_data is not None:
                assert (
                    data[0] == expected_data
                ), f"Data should match expected values for id={row_id}"
        else:
            assert len(data) == 0, f"Data should not be visible for id={row_id}"


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
@mark.parametrize("connection_factory", ["remote"], indirect=True)
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


@mark.parametrize(
    "paramstyle,query,test_data",
    [
        (
            "fb_numeric",
            'INSERT INTO "{table}" VALUES ($1, $2)',
            [(1, "alice"), (2, "bob"), (3, "charlie")],
        ),
        (
            "qmark",
            'INSERT INTO "{table}" VALUES (?, ?)',
            [(4, "david"), (5, "eve"), (6, "frank")],
        ),
    ],
)
async def test_executemany_bulk_insert_paramstyles(
    connection: Connection,
    paramstyle: str,
    query: str,
    test_data: List[Tuple],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """executemany with bulk_insert=True works correctly for both paramstyles."""
    # Set the paramstyle for this test
    original_paramstyle = firebolt.async_db.paramstyle
    firebolt.async_db.paramstyle = paramstyle
    # Generate a unique label for this test execution
    unique_label = f"test_bulk_insert_async_{paramstyle}_{randint(100000, 999999)}"
    table_name = create_drop_test_table_setup_teardown_async

    try:
        c = connection.cursor()

        # Can't do this for fb_numeric yet - FIR-49970
        if paramstyle != "fb_numeric":
            await c.execute(f"SET query_label = '{unique_label}'")

        # Execute bulk insert
        await c.executemany(
            query.format(table=table_name),
            test_data,
            bulk_insert=True,
        )

        # Verify the data was inserted correctly
        await c.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
        data = await c.fetchall()
        assert len(data) == len(test_data)
        for i, (expected_id, expected_name) in enumerate(test_data):
            assert data[i] == [expected_id, expected_name]

        # Verify that only one INSERT query was executed with our unique label
        # Can't do this for fb_numeric yet - FIR-49970
        if paramstyle != "fb_numeric":
            # Wait a moment to ensure query history is updated
            await trio.sleep(10)
            await c.execute(
                "SELECT COUNT(*) FROM information_schema.engine_query_history "
                f"WHERE query_label = '{unique_label}' AND query_text LIKE 'INSERT INTO%'"
                " AND status = 'ENDED_SUCCESSFULLY'"
            )
            query_count = (await c.fetchone())[0]
            assert (
                query_count == 1
            ), f"Expected 1 INSERT query with label '{unique_label}', but found {query_count}"
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


async def test_transaction_commit(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test transaction SQL statements with COMMIT."""
    table_name = create_drop_test_table_setup_teardown_async
    async with await connection_factory(autocommit=False) as connection:
        async with connection.cursor() as c:
            # Test successful transaction with COMMIT
            # Can't run these in autocommit off
            # result = await c.execute("BEGIN TRANSACTION")
            # assert result == 0, "BEGIN TRANSACTION should return 0 rows"

            await c.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'committed')")

            result = await c.execute("COMMIT TRANSACTION")
            assert result == 0, "COMMIT TRANSACTION should return 0 rows"

            # Verify the data was committed using separate connection
            await check_data_visibility_async(
                table_name, 1, connection_factory, True, [1, "committed"]
            )


async def test_transaction_rollback(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test transaction SQL statements with ROLLBACK."""
    table_name = create_drop_test_table_setup_teardown_async
    async with await connection_factory(autocommit=False) as connection:
        async with connection.cursor() as c:
            # Test transaction with ROLLBACK
            # Can't run these in autocommit off
            # result = await c.execute("BEGIN")  # Test short form
            # assert result == 0, "BEGIN should return 0 rows"

            await c.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'rolled_back')")

            # Verify data is visible within transaction
            await c.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
            data = await c.fetchall()
            assert len(data) == 1, "Data should be visible within transaction"

            result = await c.execute("ROLLBACK")  # Test short form
            assert result == 0, "ROLLBACK should return 0 rows"

            # Verify the data was rolled back using separate connection
            await check_data_visibility_async(table_name, 1, connection_factory, False)


async def test_transaction_cursor_isolation(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that cursors share the same transaction state - no isolation between cursors."""
    table_name = create_drop_test_table_setup_teardown_async
    async with await connection_factory(autocommit=False) as connection:
        cursor1 = connection.cursor()
        cursor2 = connection.cursor()

        # Start transaction in cursor1 and insert data
        # Can't run this in autocommit off
        # result = await cursor1.execute("BEGIN TRANSACTION")
        # assert result == 0, "BEGIN TRANSACTION should return 0 rows"

        await cursor1.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'shared_data')")

        # Verify cursor1 can see its own uncommitted data
        await cursor1.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data1 = await cursor1.fetchall()
        assert len(data1) == 1, "Cursor1 should see its own uncommitted data"
        assert data1[0] == [
            1,
            "shared_data",
        ], "Cursor1 data should match inserted values"

        # Verify cursor2 CAN see cursor1's uncommitted data (no isolation between cursors)
        await cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data2 = await cursor2.fetchall()
        assert (
            len(data2) == 1
        ), "Cursor2 should see cursor1's uncommitted data (no isolation)"
        assert data2[0] == [
            1,
            "shared_data",
        ], "Cursor2 should see the same data as cursor1"

        # Commit the transaction in cursor2 (affects both cursors)
        result = await cursor2.execute("COMMIT TRANSACTION")
        assert result == 0, "COMMIT TRANSACTION should return 0 rows"

        # Both cursors should still see the committed data
        await cursor1.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data1_after = await cursor1.fetchall()
        assert len(data1_after) == 1, "Cursor1 should see committed data"
        assert data1_after[0] == [
            1,
            "shared_data",
        ], "Cursor1 should see the committed data"


@mark.parametrize("autocommit_mode", ["implicit", "explicit"])
async def test_autocommit_immediate_visibility(
    connection: Connection,
    autocommit_mode: str,
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that statements are visible immediately with autocommit enabled (uses existing connection fixture)."""
    table_name = create_drop_test_table_setup_teardown_async
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Insert data with cursor1
    await cursor1.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_data')")

    # Immediately verify cursor2 can see the data (autocommit makes it visible)
    await cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = await cursor2.fetchall()
    assert (
        len(data) == 1
    ), f"Data should be immediately visible with {autocommit_mode} autocommit"
    assert data[0] == [1, "autocommit_data"], "Data should match inserted values"

    # Insert more data with cursor2
    await cursor2.execute(f"INSERT INTO \"{table_name}\" VALUES (2, 'more_data')")

    # Verify cursor1 can immediately see cursor2's data
    await cursor1.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
    all_data = await cursor1.fetchall()
    assert len(all_data) == 2, "All data should be immediately visible"
    assert all_data[0] == [1, "autocommit_data"], "First row should match"
    assert all_data[1] == [2, "more_data"], "Second row should match"


# Not compatible with core
@mark.parametrize("connection_factory", ["remote"], indirect=True)
async def test_begin_with_autocommit_on(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that BEGIN does not start a transaction when autocommit is enabled."""
    table_name = create_drop_test_table_setup_teardown_async

    async with await connection_factory(autocommit=True) as connection:
        cursor = connection.cursor()
        # Test that data is immediately visible without explicit transaction (autocommit)
        await cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_test')"
        )

        # Create a second cursor to verify data is visible immediately
        cursor2 = connection.cursor()
        await cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor2.fetchall()
        assert len(data) == 1, "Data should be visible immediately with autocommit"
        assert data[0] == [1, "autocommit_test"], "Data should match inserted values"

        # Now test with explicit BEGIN - this should be a no-op when autocommit is enabled
        result = await cursor.execute("BEGIN TRANSACTION")
        assert result == 0, "BEGIN TRANSACTION should return 0 rows"
        assert (
            not connection.in_transaction
        ), "Transaction should not be started when autocommit is enabled"

        await cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (2, 'no_transaction_test')"
        )

        # ROLLBACK should fail since no transaction was started
        with raises(Exception):
            await cursor.execute("ROLLBACK")

        # The second insert should not be rolled back since it was committed immediately
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 2')
        data = await cursor.fetchall()
        assert (
            len(data) == 1
        ), "Data should remain committed since no transaction was started"
        assert data[0] == [
            2,
            "no_transaction_test",
        ], "Data should match inserted values"

        # Verify data is visible from another cursor (confirming it was committed)
        cursor2 = connection.cursor()
        await cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 2')
        data = await cursor2.fetchall()
        assert len(data) == 1, "Data should be visible from other cursors"
        assert data[0] == [
            2,
            "no_transaction_test",
        ], "Data should match inserted values"


async def test_connection_commit(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that connection.commit() works correctly."""
    table_name = create_drop_test_table_setup_teardown_async

    async with await connection_factory(autocommit=False) as connection:
        cursor = connection.cursor()
        # Start a transaction
        # Can't run this in autocommit off
        # await cursor.execute("BEGIN TRANSACTION")
        await cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'commit_test')")

        # Call commit on connection level
        await connection.commit()

        # Verify data is now visible in a new cursor
        cursor2 = connection.cursor()
        await cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor2.fetchall()
        assert len(data) == 1, "Data should be visible after connection.commit()"
        assert data[0] == [1, "commit_test"], "Data should match inserted values"


async def test_connection_rollback(
    create_drop_test_table_setup_teardown_async: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that connection.rollback() works correctly."""
    table_name = create_drop_test_table_setup_teardown_async

    async with await connection_factory(autocommit=False) as connection:
        cursor = connection.cursor()
        # Start a transaction
        # Can't run this in autocommit off
        # await cursor.execute("BEGIN TRANSACTION")
        await cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (1, 'rollback_test')"
        )

        # Verify data is visible within the transaction
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor.fetchall()
        assert len(data) == 1, "Data should be visible within transaction"

        # Call rollback on connection level
        await connection.rollback()

        # Verify data is no longer visible
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor.fetchall()
        assert len(data) == 0, "Data should be rolled back after connection.rollback()"


async def test_context_manager_auto_commit_on_normal_exit(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that context manager commits transaction on normal exit when autocommit=False."""
    table_name = create_drop_test_table_setup_teardown_async

    async with await connection_factory(autocommit=False) as connection:
        cursor = connection.cursor()

        await cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (1, 'context_commit_test')"
        )
        assert connection.in_transaction, "Connection should be in transaction"

        # Verify data is visible within the transaction
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor.fetchall()
        assert len(data) == 1, "Data should be visible within transaction"
        assert data[0] == [
            1,
            "context_commit_test",
        ], "Data should match inserted values"

    # After context manager exit, transaction should be committed
    # Verify with a new connection using helper function
    await check_data_visibility_async(
        table_name, 1, connection_factory, True, [1, "context_commit_test"]
    )


async def test_context_manager_works_with_autocommit_on(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that context manager does not auto-commit when autocommit=True."""
    table_name = create_drop_test_table_setup_teardown_async

    async with await connection_factory(
        autocommit=True,  # This should prevent auto-commit behavior
    ) as connection:
        cursor = connection.cursor()

        # Insert data without explicit transaction (should commit immediately due to autocommit)
        await cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_test')"
        )

        # Verify data is immediately visible
        await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = await cursor.fetchall()
        assert len(data) == 1, "Data should be immediately visible with autocommit"

    # Verify data persists (was already committed due to autocommit)
    await check_data_visibility_async(
        table_name, 1, connection_factory, True, [1, "autocommit_test"]
    )


async def test_context_manager_no_auto_commit_on_exception_exit(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that context manager does not commit transaction on exception exit."""
    table_name = create_drop_test_table_setup_teardown_async

    try:
        async with await connection_factory(autocommit=False) as connection:
            cursor = connection.cursor()

            await cursor.execute(
                f"INSERT INTO \"{table_name}\" VALUES (1, 'exception_test')"
            )
            assert connection.in_transaction, "Connection should be in transaction"

            # Verify data is visible within the transaction
            await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
            data = await cursor.fetchall()
            assert len(data) == 1, "Data should be visible within transaction"

            # Raise an exception to trigger exception exit
            raise ValueError("Test exception")
    except ValueError:
        pass  # Expected exception

    # After exception exit, transaction should be rolled back
    # Verify with a new connection using helper function
    await check_data_visibility_async(table_name, 1, connection_factory, False)


async def test_connection_close_rollback_with_autocommit_off(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that connection.aclose() rolls back uncommitted transactions when autocommit=False."""
    table_name = create_drop_test_table_setup_teardown_async

    connection = await connection_factory(autocommit=False)

    cursor = connection.cursor()

    await cursor.execute(
        f"INSERT INTO \"{table_name}\" VALUES (1, 'close_rollback_test')"
    )
    assert connection.in_transaction, "Connection should be in transaction"

    # Verify data is visible within the transaction
    await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = await cursor.fetchall()
    assert len(data) == 1, "Data should be visible within transaction"

    # Close connection without commit - should trigger rollback
    await connection.aclose()

    # Verify transaction was rolled back with a new connection using helper function
    await check_data_visibility_async(table_name, 1, connection_factory, False)


async def test_connection_close_no_rollback_with_autocommit_on(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown_async: Callable,
) -> None:
    """Test that connection.aclose() does not rollback when autocommit=True."""
    table_name = create_drop_test_table_setup_teardown_async

    connection = await connection_factory(autocommit=True)

    cursor = connection.cursor()

    # Insert data (should commit immediately due to autocommit)
    await cursor.execute(
        f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_close_test')"
    )

    # Verify data is immediately visible
    await cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = await cursor.fetchall()
    assert len(data) == 1, "Data should be immediately visible with autocommit"

    # Close connection - should not affect already committed data
    await connection.aclose()

    # Verify data persists with a new connection using helper function
    await check_data_visibility_async(
        table_name, 1, connection_factory, True, [1, "autocommit_close_test"]
    )
