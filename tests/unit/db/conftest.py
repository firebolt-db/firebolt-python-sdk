from typing import Callable

from pytest import fixture

from firebolt.client.auth import Auth
from firebolt.db import Connection, Cursor, connect


@fixture
def connection(
    server: str,
    db_name: str,
    auth: Auth,
    engine_name: str,
    account_name: str,
    mock_connection_flow: Callable,
) -> Connection:
    mock_connection_flow()
    with connect(
        engine_name=engine_name,
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=server,
    ) as connection:
        yield connection


@fixture
def system_connection(
    server: str,
    db_name: str,
    auth: Auth,
    account_name: str,
    mock_system_connection_flow: Callable,
) -> Connection:
    mock_system_connection_flow()
    with connect(
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=server,
    ) as connection:
        yield connection


@fixture
def cursor(connection: Connection) -> Cursor:
    return connection.cursor()
