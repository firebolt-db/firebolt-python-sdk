import pytest_asyncio

from firebolt.async_db import Connection, connect


@pytest_asyncio.fixture
async def connection(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        engine_url=engine_url,
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@pytest_asyncio.fixture
async def connection_engine_name(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:

    async with await connect(
        engine_name=engine_name,
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@pytest_asyncio.fixture
async def connection_no_engine(
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:

    async with await connect(
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection
