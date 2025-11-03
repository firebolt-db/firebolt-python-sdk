import math
import time
from datetime import date, datetime
from decimal import Decimal
from random import randint
from threading import Thread
from typing import Any, Callable, List, Tuple

from pytest import mark, raises

import firebolt.db
from firebolt.client.auth import Auth
from firebolt.common._types import ColType
from firebolt.common.row_set.types import Column
from firebolt.db import Binary, Connection, Cursor, OperationalError, connect
from firebolt.utils.exception import FireboltStructuredError
from tests.integration.dbapi.conftest import LONG_SELECT_DEFAULT_V2
from tests.integration.dbapi.utils import assert_deep_eq

VALS_TO_INSERT = ",".join([f"({i},'{val}')" for (i, val) in enumerate(range(1, 360))])
LONG_INSERT = f"INSERT INTO test_tbl VALUES {VALS_TO_INSERT}"
LONG_SELECT = (
    "SELECT checksum(*) FROM GENERATE_SERIES(1, {long_value})"  # approx 6m runtime
)


def assert_deep_eq(got: Any, expected: Any, msg: str) -> bool:
    if type(got) == list and type(expected) == list:
        all([assert_deep_eq(f, s, msg) for f, s in zip(got, expected)])
    assert (
        type(got) == type(expected) and got == expected
    ), f"{msg}: {got}(got) != {expected}(expected)"


def check_data_visibility(
    table_name: str,
    row_id: int,
    connection_factory: Callable[..., Connection],
    expected_visible: bool,
    expected_data: Any = None,
) -> None:
    """Check if data is visible using a separate autocommit connection."""
    with connection_factory() as check_connection:
        cursor = check_connection.cursor()
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = {row_id}')
        data = cursor.fetchall()

        if expected_visible:
            assert len(data) == 1, f"Data should be visible for id={row_id}"
            if expected_data is not None:
                assert (
                    data[0] == expected_data
                ), f"Data should match expected values for id={row_id}"
        else:
            assert len(data) == 0, f"Data should not be visible for id={row_id}"


def test_connect_no_db(
    connection_no_db: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    test_select(
        connection_no_db,
        all_types_query,
        all_types_query_description,
        all_types_query_response,
        timezone_name,
    )


def test_select(
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
            c.execute(f"SET timezone={timezone_name}") == -1
        ), "Invalid set statment row count"

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


def test_select_inf(connection: Connection) -> None:
    with connection.cursor() as c:
        c.execute("SELECT 'inf'::float, '-inf'::float")
        data = c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"
        assert data[0][0] == float("inf"), "Invalid data returned by fetchall"
        assert data[0][1] == float("-inf"), "Invalid data returned by fetchall"


def test_select_nan(connection: Connection) -> None:
    with connection.cursor() as c:
        c.execute("SELECT 'nan'::float, '-nan'::float")
        data = c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"
        assert math.isnan(data[0][0]), "Invalid data returned by fetchall"
        assert math.isnan(data[0][1]), "Invalid data returned by fetchall"


@mark.slow
@mark.timeout(timeout=550)
def test_long_query(
    connection: Connection,
    minimal_time: Callable[[float], None],
    long_test_value: Callable[[int], int],
) -> None:
    """AWS ALB TCP timeout set to 350; make sure we handle the keepalive correctly."""

    minimal_time(350)

    with connection.cursor() as c:
        c.execute(
            LONG_SELECT.format(long_value=long_test_value(LONG_SELECT_DEFAULT_V2))
        )
        data = c.fetchall()
        assert len(data) == 1, "Invalid data size returned by fetchall"


# Not compatible with core
@mark.parametrize("connection_factory", ["remote"], indirect=True)
def test_drop_create(connection: Connection) -> None:
    """Create and drop table/index queries are handled properly."""

    def test_query(c: Cursor, query: str) -> None:
        c.execute(query)
        assert c.description == []
        assert c.rowcount == 0

    """Create table query is handled properly"""
    with connection.cursor() as c:
        # Cleanup
        c.execute('DROP AGGREGATING INDEX IF EXISTS "test_drop_create_db_agg_idx"')
        c.execute('DROP TABLE IF EXISTS "test_drop_create_tb"')
        c.execute('DROP TABLE IF EXISTS "test_drop_create_tb_dim"')

        # Fact table
        test_query(
            c,
            'CREATE FACT TABLE "test_drop_create_tb"(id int, sn string null, f float,'
            "d date, dt datetime, b bool, a array(int)) primary index id",
        )

        # Dimension table
        test_query(
            c,
            'CREATE DIMENSION TABLE "test_drop_create_tb_dim"(id int, sn string null'
            ", f float, d date, dt datetime, b bool, a array(int))",
        )

        # Create aggregating index
        test_query(
            c,
            'CREATE AGGREGATING INDEX "test_drop_create_db_agg_idx" ON '
            "test_drop_create_tb(id, count(f), count(dt))",
        )

        # Drop aggregating index
        test_query(c, 'DROP AGGREGATING INDEX "test_drop_create_db_agg_idx"')

        # Test drop once again
        test_query(c, 'DROP TABLE "test_drop_create_tb"')
        test_query(c, 'DROP TABLE IF EXISTS "test_drop_create_tb"')

        test_query(c, 'DROP TABLE "test_drop_create_tb_dim"')
        test_query(c, 'DROP TABLE IF EXISTS "test_drop_create_tb_dim"')


def test_insert(connection: Connection) -> None:
    """Insert and delete queries are handled properly."""

    def test_empty_query(c: Cursor, query: str) -> None:
        assert c.execute(query) == 0, "Invalid row count returned"
        assert c.rowcount == 0, "Invalid rowcount value"
        assert c.description == [], "Invalid description"
        assert c.fetchone() is None
        assert len(c.fetchmany()) == 0
        assert len(c.fetchall()) == 0

    with connection.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "test_insert_tb"')
        c.execute(
            'CREATE FACT TABLE "test_insert_tb"(id int, sn string null, f float,'
            "d date, dt datetime, b bool, a array(int)) primary index id"
        )

        test_empty_query(
            c,
            "INSERT INTO \"test_insert_tb\" VALUES (1, 'sn', 1.1, '2021-01-01',"
            "'2021-01-01 01:01:01', true, [1, 2, 3])",
        )

        assert (
            c.execute('SELECT * FROM "test_insert_tb" ORDER BY test_insert_tb.id') == 1
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
                    True,
                    [1, 2, 3],
                ],
            ],
            "Invalid data in table after insert",
        )


def test_parameterized_query_with_special_chars(connection: Connection) -> None:
    """Query parameters are handled properly."""
    with connection.cursor() as c:
        parameters = ["text with 'quote'", "text with \\slashes"]

        c.execute(
            "SELECT ? as one, ? as two",
            parameters,
        )

        result = c.fetchall()
        assert result == [
            [parameters[0], parameters[1]]
        ], "Invalid data in table after parameterized insert"


def test_parameterized_query(connection: Connection) -> None:
    """Query parameters are handled properly."""

    def test_empty_query(c: Cursor, query: str, params: tuple) -> None:
        assert c.execute(query, params) == 0, "Invalid row count returned"
        assert c.rowcount == 0, "Invalid rowcount value"
        assert c.description == [], "Invalid description"
        assert c.fetchone() is None
        assert len(c.fetchmany()) == 0
        assert len(c.fetchall()) == 0

    with connection.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "test_tb_parameterized"')
        c.execute(
            'CREATE FACT TABLE "test_tb_parameterized"(i int, f float, s string, sn'
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

        test_empty_query(
            c,
            'INSERT INTO "test_tb_parameterized" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,'
            " '\\?')",
            params,
        )

        # \0 is converted to 0
        params[2] = "text\\0"

        assert (
            c.execute('SELECT * FROM "test_tb_parameterized"') == 1
        ), "Invalid data length in table after parameterized insert"

        assert_deep_eq(
            c.fetchall(),
            [params + ["\\?"]],
            "Invalid data in table after parameterized insert",
        )


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
def test_executemany_bulk_insert_paramstyles(
    connection: Connection,
    paramstyle: str,
    query: str,
    test_data: List[Tuple],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """executemany with bulk_insert=True works correctly for both paramstyles."""
    # Set the paramstyle for this test
    original_paramstyle = firebolt.db.paramstyle
    firebolt.db.paramstyle = paramstyle
    # Generate a unique label for this test execution
    unique_label = f"test_bulk_insert_{paramstyle}_{randint(100000, 999999)}"
    table_name = create_drop_test_table_setup_teardown

    try:
        c = connection.cursor()

        # Can't do this for fb_numeric yet - FIR-49970
        if paramstyle != "fb_numeric":
            c.execute(f"SET query_label = '{unique_label}'")

        # Execute bulk insert
        c.executemany(
            query.format(table=table_name),
            test_data,
            bulk_insert=True,
        )

        # Verify the data was inserted correctly
        c.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
        data = c.fetchall()
        assert len(data) == len(test_data)
        for i, (expected_id, expected_name) in enumerate(test_data):
            assert data[i] == [expected_id, expected_name]

        # Verify that only one INSERT query was executed with our unique label
        # Can't do this for fb_numeric yet - FIR-49970
        if paramstyle != "fb_numeric":
            # Wait a moment to ensure query history is updated
            time.sleep(10)
            c.execute(
                "SELECT COUNT(*) FROM information_schema.engine_query_history "
                f"WHERE query_label = '{unique_label}' AND query_text LIKE 'INSERT INTO%'"
                " AND status = 'ENDED_SUCCESSFULLY'"
            )
            query_count = c.fetchone()[0]
            assert (
                query_count == 1
            ), f"Expected 1 INSERT query with label '{unique_label}', but found {query_count}"
    finally:
        firebolt.db.paramstyle = original_paramstyle


def test_multi_statement_query(connection: Connection) -> None:
    """Query parameters are handled properly"""

    with connection.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "test_tb_multi_statement"')
        c.execute(
            'CREATE FACT TABLE "test_tb_multi_statement"(i int, s string) primary index i'
        )

        c.execute(
            "INSERT INTO \"test_tb_multi_statement\" values (1, 'a'), (2, 'b');"
            'SELECT * FROM "test_tb_multi_statement";'
            'SELECT * FROM "test_tb_multi_statement" WHERE i <= 1'
        )
        assert c.description == [], "Invalid description"

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

        assert c.nextset()

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
            c.fetchall(),
            [[1, "a"]],
            "Invalid data in table after parameterized insert",
        )

        assert c.nextset() is False


def test_set_invalid_parameter(connection: Connection):
    with connection.cursor() as c:
        assert len(c._set_parameters) == 0
        with raises((OperationalError, FireboltStructuredError)) as e:
            c.execute("set some_invalid_parameter = 1")
        assert "Unknown setting" in str(e.value) or "query param not allowed" in str(
            e.value
        ), "Invalid error message"
        assert len(c._set_parameters) == 0


# Run test multiple times since the issue is flaky
@mark.parametrize("_", range(5))
def test_anyio_backend_import_issue(
    engine_name: str,
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    _: int,
) -> None:
    threads_cnt = 3
    requests_cnt = 8
    # collect threads exceptions in an array because they're ignored otherwise
    exceptions = []

    def run_query(idx: int):
        nonlocal auth, database_name, engine_name, account_name, api_endpoint
        try:
            with connect(
                auth=auth,
                database=database_name,
                account_name=account_name,
                engine_name=engine_name,
                api_endpoint=api_endpoint,
            ) as c:
                cursor = c.cursor()
                cursor.execute(f"select {idx}")
        except BaseException as e:
            exceptions.append(e)

    def run_queries_parallel() -> None:
        nonlocal requests_cnt
        threads = [Thread(target=run_query, args=(i,)) for i in range(requests_cnt)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    threads = [Thread(target=run_queries_parallel) for _ in range(threads_cnt)]

    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(exceptions) == 0, exceptions


@mark.xdist_group("multi_thread_connection_sharing")
def test_multi_thread_connection_sharing(
    engine_name: str,
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> None:
    """
    Test to verify sharing the same connection between different
    threads works. With asyncio synching an async function this used
    to fail due to a different loop having exclusive rights to the
    Httpx client. Trio fixes this issue.
    """

    exceptions = []

    connection = connect(
        auth=auth,
        database=database_name,
        account_name=account_name,
        engine_name=engine_name,
        api_endpoint=api_endpoint,
    )

    def run_query():
        try:
            cursor = connection.cursor()
            cursor.execute("select 1")
            cursor.fetchall()
        except BaseException as e:
            exceptions.append(e)

    thread_1 = Thread(target=run_query)
    thread_2 = Thread(target=run_query)

    thread_1.start()
    thread_1.join()
    thread_2.start()
    thread_2.join()

    connection.close()
    assert not exceptions


def test_bytea_roundtrip(
    connection: Connection,
) -> None:
    """Inserted and than selected bytea value doesn't get corrupted."""
    with connection.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "test_bytea_roundtrip"')
        c.execute(
            'CREATE FACT TABLE "test_bytea_roundtrip"(id int, b bytea) primary index id'
        )

        data = "bytea_123\n\tヽ༼ຈل͜ຈ༽ﾉ"

        c.execute('INSERT INTO "test_bytea_roundtrip" VALUES (1, ?)', (Binary(data),))
        c.execute('SELECT b FROM "test_bytea_roundtrip"')

        bytes_data = (c.fetchone())[0]
        assert (
            bytes_data.decode("utf-8") == data
        ), "Invalid bytea data returned after roundtrip"


def test_account_v2_connection_with_db(
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> None:
    with connect(
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        # This fails if we're not running with a db context
        connection.cursor().execute("SELECT * FROM information_schema.tables LIMIT 1")


def test_account_v2_connection_with_db_and_engine(
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
    system_cursor.execute(f'START ENGINE "{engine_name}"')
    with connect(
        database=database_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        # generate a random string to avoid name conflicts
        rnd_suffix = str(randint(0, 1000))
        cursor = connection.cursor()
        cursor.execute(f'CREATE TABLE "test_table_{rnd_suffix}" (id int)')
        # This fails if we're not running on a user engine
        cursor.execute(f'INSERT INTO "test_table_{rnd_suffix}" VALUES (1)')


def test_connection_with_mixed_case_db_and_engine(
    mixed_case_db_and_engine: Tuple[str, str],
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> None:
    test_db_name, test_engine_name = mixed_case_db_and_engine
    with connect(
        database=test_db_name,
        engine_name=test_engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS "test_table" (id int)')
        # This fails if we're not running on a user engine
        cursor.execute('INSERT INTO "test_table" VALUES (1)')


def test_select_geography(
    connection: Connection,
    select_geography_query: str,
    select_geography_description: List[Column],
    select_geography_response: List[ColType],
):
    with connection.cursor() as c:
        c.execute(select_geography_query)
        assert (
            c.description == select_geography_description
        ), "Invalid description value"
        res = c.fetchall()
        assert len(res) == 1, "Invalid data length"
        assert_deep_eq(
            res,
            select_geography_response,
            "Invalid data returned by fetchall",
        )


def test_select_struct(
    connection: Connection,
    setup_struct_query: str,
    cleanup_struct_query: str,
    select_struct_query: str,
    select_struct_description: List[Column],
    select_struct_response: List[ColType],
):
    with connection.cursor() as c:
        try:
            c.execute(setup_struct_query)
            c.execute(select_struct_query)
            assert (
                c.description == select_struct_description
            ), "Invalid description value"
            res = c.fetchall()
            assert len(res) == 1, "Invalid data length"
            assert_deep_eq(
                res,
                select_struct_response,
                "Invalid data returned by fetchall",
            )
        finally:
            c.execute(cleanup_struct_query)


def test_fb_numeric_paramstyle_all_types(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: insert/select all supported types, and parameter count errors."""
    with connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with connection.cursor() as c:
            c.execute('DROP TABLE IF EXISTS "test_fb_numeric_all_types_sync"')
            c.execute(
                'CREATE FACT TABLE "test_fb_numeric_all_types_sync" ('
                "i INT, f FLOAT, s STRING, sn STRING NULL, d DATE, dt DATETIME, b BOOL, a ARRAY(INT), dec DECIMAL(38, 3)"
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
                [1, 2, 3],  # a ARRAY(INT)
                Decimal("123.456"),  # dec DECIMAL(38, 3)
            ]
            c.execute(
                'INSERT INTO "test_fb_numeric_all_types_sync" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                params,
            )
            c.execute(
                'SELECT * FROM "test_fb_numeric_all_types_sync" WHERE i = $1', [1]
            )
            result = c.fetchall()
            # None is returned as None, arrays as lists, decimals as Decimal
            assert result == [params]


def test_fb_numeric_paramstyle_not_enough_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: not enough parameters supplied."""
    with connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with connection.cursor() as c:
            with raises(FireboltStructuredError) as exc_info:
                c.execute("SELECT $1, $2", [1])
            assert (
                "query referenced positional parameter $2, but it was not set"
                in str(exc_info.value).lower()
            )


def test_fb_numeric_paramstyle_too_many_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    """Test fb_numeric paramstyle: too many parameters supplied (should succeed)."""
    with connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        with connection.cursor() as c:
            c.execute('DROP TABLE IF EXISTS "test_fb_numeric_params2_sync"')
            c.execute(
                'CREATE FACT TABLE "test_fb_numeric_params2_sync" (i INT, s STRING)'
            )
            # Three params for two placeholders: should succeed, extra param ignored
            c.execute(
                'INSERT INTO "test_fb_numeric_params2_sync" VALUES ($1, $2)',
                [1, "foo", 123],
            )
            c.execute('SELECT * FROM "test_fb_numeric_params2_sync" WHERE i = $1', [1])
            result = c.fetchall()
            assert result == [[1, "foo"]]


def test_fb_numeric_paramstyle_incorrect_params(
    engine_name, database_name, auth, account_name, api_endpoint, fb_numeric_paramstyle
):
    with connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        c = connection.cursor()
        with raises(FireboltStructuredError) as exc_info:
            c.execute(
                "SELECT $34, $72",
                [1, "foo"],
            )
        assert "Query referenced positional parameter $34, but it was not set" in str(
            exc_info.value
        )


def test_select_quoted_decimal(
    connection: Connection, long_decimal_value: str, long_value_decimal_sql: str
):
    with connection.cursor() as c:
        c.execute(long_value_decimal_sql)
        result = c.fetchall()
        assert len(result) == 1, "Invalid data length returned by fetchall"
        assert result[0][0] == Decimal(
            long_decimal_value
        ), "Invalid data returned by fetchall"


def test_select_quoted_bigint(
    connection: Connection, long_bigint_value: str, long_value_bigint_sql: str
):
    with connection.cursor() as c:
        c.execute(long_value_bigint_sql)
        result = c.fetchall()
        assert len(result) == 1, "Invalid data length returned by fetchall"
        assert result[0][0] == int(
            long_bigint_value
        ), "Invalid data returned by fetchall"


def test_transaction_commit(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test transaction SQL statements with COMMIT."""
    table_name = create_drop_test_table_setup_teardown
    with connection_factory(autocommit=False) as connection_autocommit_off:
        with connection_autocommit_off.cursor() as c:
            # Test successful transaction with COMMIT
            # Can't run these in autocommit off
            # result = c.execute("BEGIN TRANSACTION")
            # assert result == 0, "BEGIN TRANSACTION should return 0 rows"

            c.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'committed')")

            result = c.execute("COMMIT TRANSACTION")
            assert result == 0, "COMMIT TRANSACTION should return 0 rows"

            # Verify the data was committed using separate connection
            check_data_visibility(
                table_name, 1, connection_factory, True, [1, "committed"]
            )


def test_transaction_rollback(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test transaction SQL statements with ROLLBACK."""
    table_name = create_drop_test_table_setup_teardown
    with connection_factory(autocommit=False) as connection_autocommit_off:
        with connection_autocommit_off.cursor() as c:
            # Test transaction with ROLLBACK
            # Can't run these with autocommit off
            # result = c.execute("BEGIN")  # Test short form
            # assert result == 0, "BEGIN should return 0 rows"

            c.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'rolled_back')")

            # Verify data is visible within transaction
            c.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
            data = c.fetchall()
            assert len(data) == 1, "Data should be visible within transaction"

            result = c.execute("ROLLBACK")  # Test short form
            assert result == 0, "ROLLBACK should return 0 rows"

            # Verify the data was rolled back using separate connection
            check_data_visibility(table_name, 1, connection_factory, False)


def test_transaction_cursor_isolation(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that cursors share the same transaction state - no isolation between cursors."""
    table_name = create_drop_test_table_setup_teardown
    with connection_factory(autocommit=False) as connection_autocommit_off:
        cursor1 = connection_autocommit_off.cursor()
        cursor2 = connection_autocommit_off.cursor()

        # Start transaction in cursor1 and insert data
        # Can't run this in autocommit off
        # result = cursor1.execute("BEGIN TRANSACTION")
        # assert result == 0, "BEGIN TRANSACTION should return 0 rows"

        cursor1.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'shared_data')")

        # Verify cursor1 can see its own uncommitted data
        cursor1.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data1 = cursor1.fetchall()
        assert len(data1) == 1, "Cursor1 should see its own uncommitted data"
        assert data1[0] == [
            1,
            "shared_data",
        ], "Cursor1 data should match inserted values"

        # Verify cursor2 CAN see cursor1's uncommitted data (no isolation between cursors)
        cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data2 = cursor2.fetchall()
        assert (
            len(data2) == 1
        ), "Cursor2 should see cursor1's uncommitted data (no isolation)"
        assert data2[0] == [
            1,
            "shared_data",
        ], "Cursor2 should see the same data as cursor1"

        # Commit the transaction in cursor2 (affects both cursors)
        result = cursor2.execute("COMMIT TRANSACTION")
        assert result == 0, "COMMIT TRANSACTION should return 0 rows"

        # Both cursors should still see the committed data
        cursor1.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data1_after = cursor1.fetchall()
        assert len(data1_after) == 1, "Cursor1 should see committed data"
        assert data1_after[0] == [
            1,
            "shared_data",
        ], "Cursor1 should see the committed data"


@mark.parametrize("autocommit_mode", ["implicit", "explicit"])
def test_autocommit_immediate_visibility(
    connection: Connection,
    autocommit_mode: str,
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that statements are visible immediately with autocommit enabled (uses existing connection fixture)."""
    table_name = create_drop_test_table_setup_teardown
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Insert data with cursor1
    cursor1.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_data')")

    # Immediately verify cursor2 can see the data (autocommit makes it visible)
    cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = cursor2.fetchall()
    assert (
        len(data) == 1
    ), f"Data should be immediately visible with {autocommit_mode} autocommit"
    assert data[0] == [1, "autocommit_data"], "Data should match inserted values"

    # Insert more data with cursor2
    cursor2.execute(f"INSERT INTO \"{table_name}\" VALUES (2, 'more_data')")

    # Verify cursor1 can immediately see cursor2's data
    cursor1.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
    all_data = cursor1.fetchall()
    assert len(all_data) == 2, "All data should be immediately visible"
    assert all_data[0] == [1, "autocommit_data"], "First row should match"
    assert all_data[1] == [2, "more_data"], "Second row should match"


# Not compatible with core
@mark.parametrize("connection_factory", ["remote"], indirect=True)
def test_begin_with_autocommit_on(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that BEGIN does not start a transaction when autocommit is enabled."""
    table_name = create_drop_test_table_setup_teardown

    with connection_factory(autocommit=True) as connection:
        cursor = connection.cursor()
        # Test that data is immediately visible without explicit transaction (autocommit)
        cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_test')")

        # Create a second cursor to verify data is visible immediately
        cursor2 = connection.cursor()
        cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor2.fetchall()
        assert len(data) == 1, "Data should be visible immediately with autocommit"
        assert data[0] == [1, "autocommit_test"], "Data should match inserted values"

        # Now test with explicit BEGIN - this should be a no-op when autocommit is enabled
        result = cursor.execute("BEGIN TRANSACTION")
        assert result == 0, "BEGIN TRANSACTION should return 0 rows"
        assert (
            not connection.in_transaction
        ), "Transaction should not be started when autocommit is enabled"

        cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (2, 'no_transaction_test')"
        )

        # ROLLBACK should fail since no transaction was started
        with raises(Exception):
            cursor.execute("ROLLBACK")

        # The second insert should not be rolled back since it was committed immediately
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 2')
        data = cursor.fetchall()
        assert (
            len(data) == 1
        ), "Data should remain committed since no transaction was started"
        assert data[0] == [
            2,
            "no_transaction_test",
        ], "Data should match inserted values"

        # Verify data is visible from another cursor (confirming it was committed)
        cursor2 = connection.cursor()
        cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 2')
        data = cursor2.fetchall()
        assert len(data) == 1, "Data should be visible from other cursors"
        assert data[0] == [
            2,
            "no_transaction_test",
        ], "Data should match inserted values"


def test_connection_commit(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that connection.commit() works correctly."""
    table_name = create_drop_test_table_setup_teardown

    with connection_factory(autocommit=False) as connection_autocommit_off:
        cursor = connection_autocommit_off.cursor()
        # Start a transaction
        # Can't run this in autocommit off
        # cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'commit_test')")

        # Call commit on connection level
        connection_autocommit_off.commit()

        # Verify data is now visible in a new cursor
        cursor2 = connection_autocommit_off.cursor()
        cursor2.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor2.fetchall()
        assert len(data) == 1, "Data should be visible after connection.commit()"
        assert data[0] == [1, "commit_test"], "Data should match inserted values"


def test_connection_rollback(
    create_drop_test_table_setup_teardown: Callable,
    connection_factory: Callable[..., Connection],
) -> None:
    """Test that connection.rollback() works correctly."""
    table_name = create_drop_test_table_setup_teardown

    with connection_factory(autocommit=False) as connection_autocommit_off:
        cursor = connection_autocommit_off.cursor()
        # Start a transaction
        # cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'rollback_test')")

        # Verify data is visible within the transaction
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor.fetchall()
        assert len(data) == 1, "Data should be visible within transaction"

        # Call rollback on connection level
        connection_autocommit_off.rollback()

        # Verify data is no longer visible
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor.fetchall()
        assert len(data) == 0, "Data should be rolled back after connection.rollback()"


def test_context_manager_auto_commit_on_normal_exit(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that context manager commits transaction on normal exit when autocommit=False."""
    table_name = create_drop_test_table_setup_teardown

    with connection_factory(autocommit=False) as connection:
        cursor = connection.cursor()

        cursor.execute(
            f"INSERT INTO \"{table_name}\" VALUES (1, 'context_commit_test')"
        )
        assert connection.in_transaction, "Connection should be in transaction"

        # Verify data is visible within the transaction
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor.fetchall()
        assert len(data) == 1, "Data should be visible within transaction"
        assert data[0] == [
            1,
            "context_commit_test",
        ], "Data should match inserted values"

    # After context manager exit, transaction should be committed
    # Verify with a new connection using helper function
    check_data_visibility(
        table_name, 1, connection_factory, True, [1, "context_commit_test"]
    )


def test_context_manager_works_with_autocommit_on(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that context manager does not auto-commit when autocommit=True."""
    table_name = create_drop_test_table_setup_teardown

    with connection_factory(
        autocommit=True,  # This should prevent auto-commit behavior
    ) as connection:
        cursor = connection.cursor()

        # Insert data without explicit transaction (should commit immediately due to autocommit)
        cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_test')")

        # Verify data is immediately visible
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
        data = cursor.fetchall()
        assert len(data) == 1, "Data should be immediately visible with autocommit"

    # Verify data persists (was already committed due to autocommit)
    check_data_visibility(
        table_name, 1, connection_factory, True, [1, "autocommit_test"]
    )


def test_context_manager_no_auto_commit_on_exception_exit(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that context manager does not commit transaction on exception exit."""
    table_name = create_drop_test_table_setup_teardown

    try:
        with connection_factory(autocommit=False) as connection:
            cursor = connection.cursor()

            cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'exception_test')")
            assert connection.in_transaction, "Connection should be in transaction"

            # Verify data is visible within the transaction
            cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
            data = cursor.fetchall()
            assert len(data) == 1, "Data should be visible within transaction"

            # Raise an exception to trigger exception exit
            raise ValueError("Test exception")
    except ValueError:
        pass  # Expected exception

    # After exception exit, transaction should be rolled back
    # Verify with a new connection using helper function
    check_data_visibility(table_name, 1, connection_factory, False)


def test_connection_close_rollback_with_autocommit_off(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that connection.close() rolls back uncommitted transactions when autocommit=False."""
    table_name = create_drop_test_table_setup_teardown

    connection = connection_factory(autocommit=False)

    cursor = connection.cursor()

    cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'close_rollback_test')")
    assert connection.in_transaction, "Connection should be in transaction"

    # Verify data is visible within the transaction
    cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = cursor.fetchall()
    assert len(data) == 1, "Data should be visible within transaction"

    # Close connection without commit - should trigger rollback
    connection.close()

    # Verify transaction was rolled back with a new connection using helper function
    check_data_visibility(table_name, 1, connection_factory, False)


def test_connection_close_no_rollback_with_autocommit_on(
    connection_factory: Callable[..., Connection],
    create_drop_test_table_setup_teardown: Callable,
) -> None:
    """Test that connection.close() does not rollback when autocommit=True."""
    table_name = create_drop_test_table_setup_teardown

    connection = connection_factory(autocommit=True)

    cursor = connection.cursor()

    # Insert data (should commit immediately due to autocommit)
    cursor.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'autocommit_close_test')")

    # Verify data is immediately visible
    cursor.execute(f'SELECT * FROM "{table_name}" WHERE id = 1')
    data = cursor.fetchall()
    assert len(data) == 1, "Data should be immediately visible with autocommit"

    # Close connection - should not affect already committed data
    connection.close()

    # Verify data persists with a new connection using helper function
    check_data_visibility(
        table_name, 1, connection_factory, True, [1, "autocommit_close_test"]
    )
