from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from logging import getLogger
from typing import List

from pytest import fixture

from firebolt.async_db.cursor import Column
from firebolt.common._types import ColType
from firebolt.db import ARRAY, DECIMAL, Connection

LOGGER = getLogger(__name__)

CREATE_TEST_TABLE = (
    "CREATE DIMENSION TABLE IF NOT EXISTS test_tbl (id int, name string)"
)
DROP_TEST_TABLE = "DROP TABLE IF EXISTS test_tbl CASCADE"


@fixture
def create_drop_test_table_setup_teardown(connection: Connection) -> None:
    with connection.cursor() as c:
        c.execute(CREATE_TEST_TABLE)
        yield c
        c.execute(DROP_TEST_TABLE)


@fixture
async def create_server_side_test_table_setup_teardown_async(
    connection: Connection,
) -> None:
    with connection.cursor() as c:
        await c.execute(CREATE_TEST_TABLE)
        yield c
        await c.execute(DROP_TEST_TABLE)


@fixture
def create_drop_test_table_setup_teardown(connection: Connection) -> None:
    with connection.cursor() as c:
        c.execute(CREATE_TEST_TABLE)
        yield c
        c.execute(DROP_TEST_TABLE)


@fixture
async def create_drop_test_table_setup_teardown_async(connection: Connection) -> None:
    with connection.cursor() as c:
        await c.execute(CREATE_TEST_TABLE)
        yield c
        await c.execute(DROP_TEST_TABLE)


@fixture
def all_types_query() -> str:
    return (
        "select 1 as uint8, "
        "-1 as int_8, "  # int8 is a reserved keyword
        "257 as uint16, "
        "-257 as int16, "
        "80000 as uint32, "
        "-80000 as int32, "
        "30000000000 as uint64, "
        "-30000000000 as int64, "
        "cast(1.23 AS FLOAT) as float32, "
        "1.23456789012 as float64, "
        "'text' as \"string\", "
        "CAST('2021-03-28' AS DATE) as \"date\", "
        "pgdate '0001-01-01' as \"pgdate\", "
        "CAST('2019-07-31 01:01:01' AS DATETIME) as \"datetime\", "
        "CAST('1111-01-05 17:04:42.123456' as timestampntz) as \"timestampntz\", "
        "'1111-01-05 17:04:42.123456'::timestamptz as \"timestamptz\", "
        'true as "boolean", '
        "[1,2,3,4] as \"array\", cast('1231232.123459999990457054844258706536' as "
        'decimal(38,30)) as "decimal", '
        'cast(null as  int) as "nullable", '
        "'abc123'::bytea as \"bytea\""
    )


@fixture
def all_types_query_description() -> List[Column]:
    return [
        Column("uint8", int, None, None, None, None, None),
        Column("int_8", int, None, None, None, None, None),
        Column("uint16", int, None, None, None, None, None),
        Column("int16", int, None, None, None, None, None),
        Column("uint32", int, None, None, None, None, None),
        Column("int32", int, None, None, None, None, None),
        Column("uint64", int, None, None, None, None, None),
        Column("int64", int, None, None, None, None, None),
        Column("float32", float, None, None, None, None, None),
        Column("float64", float, None, None, None, None, None),
        Column("string", str, None, None, None, None, None),
        Column("date", date, None, None, None, None, None),
        Column("pgdate", date, None, None, None, None, None),
        Column("datetime", datetime, None, None, None, None, None),
        Column("timestampntz", datetime, None, None, None, None, None),
        Column("timestamptz", datetime, None, None, None, None, None),
        Column("boolean", bool, None, None, None, None, None),
        Column("array", ARRAY(int), None, None, None, None, None),
        Column("decimal", DECIMAL(38, 30), None, None, None, None, None),
        Column("nullable", int, None, None, None, None, None),
        Column("bytea", bytes, None, None, None, None, None),
    ]


@fixture
def all_types_query_response(timezone_offset_seconds: int) -> List[ColType]:
    return [
        [
            1,
            -1,
            257,
            -257,
            80000,
            -80000,
            30000000000,
            -30000000000,
            1.23,
            1.23456789012,
            "text",
            date(2021, 3, 28),
            date(1, 1, 1),
            datetime(2019, 7, 31, 1, 1, 1),
            datetime(1111, 1, 5, 17, 4, 42, 123456),
            datetime(
                1111,
                1,
                5,
                17,
                4,
                42,
                123456,
                tzinfo=timezone(timedelta(seconds=timezone_offset_seconds)),
            ),
            True,
            [1, 2, 3, 4],
            Decimal("1231232.123459999990457054844258706536"),
            None,
            b"abc123",
        ]
    ]


@fixture
def timezone_name() -> str:
    return "Asia/Calcutta"


@fixture
def timezone_offset_seconds() -> int:
    return 5 * 3600 + 53 * 60 + 28  # 05:53:28


@fixture
def create_drop_description() -> List[Column]:
    return [
        Column("host", str, None, None, None, None, None),
        Column("port", int, None, None, None, None, None),
        Column("status", int, None, None, None, None, None),
        Column("error", str, None, None, None, None, None),
        Column("num_hosts_remaining", int, None, None, None, None, None),
        Column("num_hosts_active", int, None, None, None, None, None),
    ]
