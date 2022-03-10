from datetime import date, datetime
from decimal import Decimal
from logging import getLogger
from typing import List

from pytest import fixture

from firebolt.async_db._types import ColType
from firebolt.async_db.cursor import Column
from firebolt.db import ARRAY, DATETIME64, DECIMAL

LOGGER = getLogger(__name__)


@fixture
def all_types_query() -> str:
    return (
        "select 1 as uint8, "
        "-1 as int8, "
        "257 as uint16, "
        "-257 as int16, "
        "80000 as uint32, "
        "-80000 as int32, "
        "30000000000 as uint64, "
        "-30000000000 as int64, "
        "cast(1.23 AS FLOAT) as float32, "
        "1.2345678901234 as float64, "
        "'text' as \"string\", "
        "CAST('2021-03-28' AS DATE) as \"date\", "
        "CAST('1860-03-04' AS DATE_EXT) as \"date32\","
        "CAST('2019-07-31 01:01:01' AS DATETIME) as \"datetime\", "
        "CAST('2019-07-31 01:01:01.1234' AS TIMESTAMP_EXT(4)) as \"datetime64\", "
        'true as "bool",'
        '[1,2,3,4] as "array", cast(1231232.123459999990457054844258706536 as '
        'decimal(38,30)) as "decimal", '
        "cast(null as  int) as nullable"
    )


@fixture
def all_types_query_description() -> List[Column]:
    return [
        Column("uint8", int, None, None, None, None, None),
        Column("int8", int, None, None, None, None, None),
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
        Column("date32", date, None, None, None, None, None),
        Column("datetime", datetime, None, None, None, None, None),
        Column("datetime64", DATETIME64(4), None, None, None, None, None),
        Column("bool", int, None, None, None, None, None),
        Column("array", ARRAY(int), None, None, None, None, None),
        Column("decimal", DECIMAL(38, 30), None, None, None, None, None),
        Column("nullable", int, None, None, None, None, None),
    ]


@fixture
def all_types_query_response() -> List[ColType]:
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
            date(1860, 3, 4),
            datetime(2019, 7, 31, 1, 1, 1),
            datetime(2019, 7, 31, 1, 1, 1, 123400),
            1,
            [1, 2, 3, 4],
            Decimal("1231232.123459999990457054844258706536"),
            None,
        ]
    ]


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
