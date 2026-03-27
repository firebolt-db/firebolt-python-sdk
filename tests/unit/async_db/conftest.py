from typing import AsyncGenerator

from pytest import fixture

import firebolt.async_db
from firebolt.async_db import Connection, Cursor, connect
from firebolt.client.auth import Auth
from tests.unit.db_conftest import *  # noqa


@fixture
async def connection(
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    engine_name: str,
    account_name: str,
    mock_connection_flow: Callable,
) -> AsyncGenerator[Connection, None]:
    mock_connection_flow()
    async with (
        await connect(
            engine_name=engine_name,
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        )
    ) as connection:
        # cache account_id for tests
        await connection._client.account_id
        yield connection


@fixture
async def cursor(connection: Connection) -> Cursor:
    return connection.cursor()


@fixture
async def connection_autocommit_off(
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    engine_name: str,
    account_name: str,
    mock_connection_flow: Callable,
) -> AsyncGenerator[Connection, None]:
    """Connection fixture with autocommit=False for transaction testing."""
    mock_connection_flow()
    async with (
        await connect(
            engine_name=engine_name,
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            autocommit=False,
        )
    ) as connection:
        # cache account_id for tests
        await connection._client.account_id
        yield connection


@fixture
def fb_numeric_paramstyle():
    """Fixture that sets paramstyle to fb_numeric and resets it after the test."""
    original_paramstyle = firebolt.async_db.paramstyle
    firebolt.async_db.paramstyle = "fb_numeric"
    yield
    firebolt.async_db.paramstyle = original_paramstyle
