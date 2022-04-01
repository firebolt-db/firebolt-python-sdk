from pytest import fixture

from firebolt.db import Connection, connect


@fixture
def connection(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        engine_url=engine_url,
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()


@fixture
def connection_engine_name(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        engine_name=engine_name,
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()


@fixture
def connection_no_engine(
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    connection = connect(
        database=database_name,
        username=username,
        password=password,
        account_name=account_name,
        api_endpoint=api_endpoint,
    )
    yield connection
    connection.close()
