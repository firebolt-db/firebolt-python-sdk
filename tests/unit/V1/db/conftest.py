from pytest import fixture

from firebolt.client import ClientV1 as Client
from firebolt.client.auth.base import Auth
from firebolt.db import Connection, Cursor, connect


@fixture
def auth(username_password_auth) -> Auth:
    return username_password_auth


@fixture
def connection(
    api_endpoint: str, db_name: str, username_password_auth: Auth
) -> Connection:
    with connect(
        engine_url=api_endpoint,
        database=db_name,
        auth=username_password_auth,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def cursor(connection: Connection) -> Cursor:
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
