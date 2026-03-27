from typing import Callable

from pytest import fixture

import firebolt.db
from firebolt.client.auth import Auth
from firebolt.db import Connection, Cursor, connect


@fixture
def connection(
    api_endpoint: str,
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
        api_endpoint=api_endpoint,
    ) as connection:
        # cache account_id for tests
        connection._client.account_id
        yield connection


@fixture
def system_connection(
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    account_name: str,
    mock_system_engine_connection_flow: Callable,
) -> Connection:
    mock_system_engine_connection_flow()
    with connect(
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def cursor(connection: Connection) -> Cursor:
    return connection.cursor()


@fixture
def connection_autocommit_off(
    api_endpoint: str,
    db_name: str,
    auth: Auth,
    engine_name: str,
    account_name: str,
    mock_connection_flow: Callable,
):
    """Connection fixture with autocommit=False for transaction testing."""
    mock_connection_flow()
    with connect(
        engine_name=engine_name,
        database=db_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
        autocommit=False,
    ) as connection:
        # cache account_id for tests
        connection._client.account_id
        yield connection


@fixture
def fb_numeric_paramstyle():
    """Fixture that sets paramstyle to fb_numeric and resets it after the test."""
    original_paramstyle = firebolt.db.paramstyle
    firebolt.db.paramstyle = "fb_numeric"
    yield
    firebolt.db.paramstyle = original_paramstyle
