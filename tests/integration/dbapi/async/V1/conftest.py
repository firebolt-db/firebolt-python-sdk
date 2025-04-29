from pytest import fixture

from firebolt.async_db import Connection, connect
from firebolt.client.auth.base import Auth


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
