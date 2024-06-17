from pytest import fixture

from firebolt.async_db import Connection, Cursor, connect
from firebolt.client import AsyncClientV1 as Client
from firebolt.client.auth.base import Auth
from firebolt.client.auth.username_password import UsernamePassword


@fixture
async def connection(
    api_endpoint: str, db_name: str, username_password_auth: UsernamePassword
) -> Connection:
    async with (
        await connect(
            engine_url=api_endpoint,
            database=db_name,
            auth=username_password_auth,
            api_endpoint=api_endpoint,
        )
    ) as connection:
        yield connection


@fixture
async def cursor(connection: Connection) -> Cursor:
    return connection.cursor()


@fixture
def client(
    api_endpoint: str,
    account_name: str,
    auth: Auth,
) -> Client:
    return Client(
        account_name=account_name,
        auth=auth,
        api_endpoint=api_endpoint,
    )
