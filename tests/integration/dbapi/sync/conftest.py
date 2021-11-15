from pytest import fixture

from firebolt.db import Connection, connect


@fixture
def connection(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> Connection:
    return connect(
        engine_url=engine_url,
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
    )


@fixture
def connection_engine_name(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
) -> Connection:
    return connect(
        engine_name=engine_name,
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
    )
