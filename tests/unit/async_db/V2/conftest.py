from pytest import fixture

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
) -> Connection:
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
