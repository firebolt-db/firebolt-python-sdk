from _pytest.fixtures import SubRequest
from pytest import fixture

from firebolt.client.auth import Auth, UsernamePassword
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
    with connect(
        engine_url=engine_url,
        database=database_name,
        auth=UsernamePassword(username, password),
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture(params=("engine_name", "engine_url", "no_engine"))
def any_engine_connection(
    engine_name: str,
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    account_name: str,
    api_endpoint: str,
    request: SubRequest,
) -> Connection:
    args = {
        "engine_name": {"engine_name": engine_name},
        "engine_url": {"engine_url": engine_url},
        "no_engine": {},
    }
    with connect(
        database=database_name,
        auth=UsernamePassword(username, password),
        account_name=account_name,
        api_endpoint=api_endpoint,
        **args[request.param],
    ) as connection:
        yield connection


@fixture
def any_auth_connection(
    engine_url: str,
    database_name: str,
    any_auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        engine_url=engine_url,
        database=database_name,
        auth=any_auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection
