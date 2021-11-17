from pytest import fixture

from firebolt.async_db import Connection, connect


@fixture
async def connection(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> Connection:
    async with await connect(
        engine_url=engine_url,
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
        account_name=account_name,
    ) as connection:
        yield connection


@fixture
async def connection_engine_name(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> Connection:

    async with await connect(
        engine_name=engine_name,
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
        account_name=account_name,
    ) as connection:
        yield connection
