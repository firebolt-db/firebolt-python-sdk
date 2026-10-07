from typing import Any, Callable, Tuple

from pytest import fixture

import firebolt.db
from firebolt.client.auth.base import Auth
from firebolt.db import Connection, connect


@fixture
def connection(
    connection_factory: Callable[..., Connection],
) -> Connection:
    with connection_factory() as connection:
        yield connection


@fixture
def connection_autocommit_off(
    connection_factory: Callable[..., Connection],
) -> Connection:
    with connection_factory(autocommit=False) as connection:
        yield connection


@fixture(params=["remote", "core"])
def connection_factory(
    engine_name: str,
    database_name: str,
    auth: Auth,
    core_auth: Auth,
    account_name: str,
    api_endpoint: str,
    core_url: str,
    request: Any,
) -> Callable[..., Connection]:
    def factory(**kwargs: Any) -> Connection:
        if request.param == "core":
            base_kwargs = {
                "database": kwargs.pop("database", "firebolt"),
                "auth": core_auth,
                "url": core_url,
            }
        else:
            base_kwargs = {
                "engine_name": engine_name,
                "database": kwargs.pop("database", database_name),
                "auth": auth,
                "account_name": account_name,
                "api_endpoint": api_endpoint,
            }
        return connect(**base_kwargs, **kwargs)

    return factory


@fixture
def connection_no_db(
    engine_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def connection_system_engine(
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def connection_system_engine_no_db(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
def mixed_case_db_and_engine(
    connection_system_engine: Connection,
    database_name: str,
    engine_name: str,
) -> Tuple[str, str]:
    test_db_name = f"{database_name}_MixedCase"
    test_engine_name = f"{engine_name}_MixedCase"
    system_cursor = connection_system_engine.cursor()
    system_cursor.execute(f'CREATE DATABASE "{test_db_name}"')
    system_cursor.execute(f'CREATE ENGINE "{test_engine_name}"')

    yield test_db_name, test_engine_name

    system_cursor.execute(f'DROP DATABASE "{test_db_name}"')
    system_cursor.execute(f'STOP ENGINE "{test_engine_name}"')
    system_cursor.execute(f'DROP ENGINE "{test_engine_name}"')


@fixture
def fb_numeric_paramstyle():
    """Fixture that sets paramstyle to fb_numeric and resets it after the test."""
    original_paramstyle = firebolt.db.paramstyle
    firebolt.db.paramstyle = "fb_numeric"
    yield
    firebolt.db.paramstyle = original_paramstyle
