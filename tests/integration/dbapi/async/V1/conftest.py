from decimal import Decimal
from typing import List

from pytest import fixture

from firebolt.async_db import Connection, connect
from firebolt.client.auth.base import Auth
from firebolt.common._types import ColType


@fixture
async def username_password_connection(
    engine_url: str,
    database_name: str,
    password_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        engine_url=engine_url,
        database=database_name,
        auth=password_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def connection(
    engine_url: str,
    database_name: str,
    password_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        engine_url=engine_url,
        database=database_name,
        auth=password_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def connection_engine_name(
    engine_name: str,
    database_name: str,
    password_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:

    async with await connect(
        engine_name=engine_name,
        database=database_name,
        auth=password_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def connection_no_engine(
    database_name: str,
    password_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:

    async with await connect(
        database=database_name,
        auth=password_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def all_types_query_response_v1(
    all_types_query_response: List[List[ColType]],
) -> List[List[ColType]]:
    """
    V1 still returns decimals as floats, despite overflow. That's why it's not fully accurate.
    """
    all_types_query_response[0][18] = Decimal("1231232.1234599999152123928070068359375")
    return all_types_query_response
