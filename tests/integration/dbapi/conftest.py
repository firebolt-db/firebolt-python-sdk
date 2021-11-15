from datetime import date, datetime
from logging import getLogger
from os import environ
from typing import List

from pytest import fixture

from firebolt.async_db._types import ColType
from firebolt.async_db.cursor import Column
from firebolt.db import ARRAY

LOGGER = getLogger(__name__)

ENGINE_URL_ENV = "ENGINE_URL"
ENGINE_NAME_ENV = "ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
USERNAME_ENV = "USERNAME"
PASSWORD_ENV = "PASSWORD"
API_ENDPOINT_ENV = "API_ENDPOINT"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_url() -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def username() -> str:
    return must_env(USERNAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture
def all_types_query() -> str:
    return (
        "select 1 as uint8, 258 as uint16, 80000 as uint32, -30000 as int32,"
        "30000000000 as uint64, -30000000000 as int64, cast(1.23 AS FLOAT) as float32,"
        " 1.2345678901234 as float64, 'text' as \"string\", "
        "CAST('2021-03-28' AS DATE) as \"date\", "
        'CAST(\'2019-07-31 01:01:01\' AS DATETIME) as "datetime", true as "bool",'
        '[1,2,3,4] as "array", cast(null as int) as nullable'
    )


@fixture
def all_types_query_description() -> List[Column]:
    return [
        Column("uint8", int, None, None, None, None, None),
        Column("uint16", int, None, None, None, None, None),
        Column("uint32", int, None, None, None, None, None),
        Column("int32", int, None, None, None, None, None),
        Column("uint64", int, None, None, None, None, None),
        Column("int64", int, None, None, None, None, None),
        Column("float32", float, None, None, None, None, None),
        Column("float64", float, None, None, None, None, None),
        Column("string", str, None, None, None, None, None),
        Column("date", date, None, None, None, None, None),
        Column("datetime", datetime, None, None, None, None, None),
        Column("bool", int, None, None, None, None, None),
        Column("array", ARRAY(int), None, None, None, None, None),
        Column("nullable", str, None, None, None, None, None),
    ]


@fixture
def all_types_query_response() -> List[ColType]:
    return [
        [
            1,
            258,
            80000,
            -30000,
            30000000000,
            -30000000000,
            1.23,
            1.23456789012,
            "text",
            date(2021, 3, 28),
            datetime(2019, 7, 31, 1, 1, 1),
            1,
            [1, 2, 3, 4],
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
