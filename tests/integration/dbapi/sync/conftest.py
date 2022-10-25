from pytest import fixture

from firebolt.client.auth.base import Auth
from firebolt.db import Connection, connect


@fixture
def username_password_connection(
    engine_url: str,
    database_name: str,
    password_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        engine_url=engine_url,
        database=database_name,
        auth=password_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()


@fixture
async def connection(
    engine_url: str,
    database_name: str,
    service_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        engine_url=engine_url,
        database=database_name,
        auth=service_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()


@fixture
def connection_engine_name(
    engine_name: str,
    database_name: str,
    service_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        engine_name=engine_name,
        database=database_name,
        auth=service_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()


@fixture
def connection_no_engine(
    database_name: str,
    service_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        database=database_name,
        auth=service_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()
