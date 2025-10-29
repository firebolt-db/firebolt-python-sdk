import random
import string
from typing import Any, Callable, Tuple

from pytest import fixture

import firebolt.async_db
from firebolt.async_db import Connection, connect
from firebolt.client.auth.base import Auth
from firebolt.client.auth.client_credentials import ClientCredentials
from tests.integration.conftest import Secret


@fixture
async def connection(
    connection_factory: Callable[..., Connection],
) -> Connection:
    async with await connection_factory() as connection:
        yield connection


@fixture(params=["remote", "core"])
async def connection_factory(
    engine_name: str,
    database_name: str,
    auth: Auth,
    core_auth: Auth,
    account_name: str,
    api_endpoint: str,
    core_url: str,
    request: Any,
) -> Callable[..., Connection]:
    async def factory(**kwargs: Any) -> Connection:
        if request.param == "core":
            base_kwargs = {
                "database": "firebolt",
                "auth": core_auth,
                "url": core_url,
            }
        else:
            base_kwargs = {
                "engine_name": engine_name,
                "database": database_name,
                "auth": auth,
                "account_name": account_name,
                "api_endpoint": api_endpoint,
            }
        return await connect(**base_kwargs, **kwargs)

    return factory


@fixture
async def connection_no_db(
    engine_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def connection_system_engine(
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def connection_system_engine_no_db(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    async with await connect(
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture
async def service_account_no_user(
    connection_system_engine_no_db: Connection,
    database_name: str,
) -> Tuple[str, Secret]:
    # function-level fixture so we need to make sa name is unique
    randomness = "".join(random.choices(string.ascii_letters + string.digits, k=2))
    sa_account_name = f"{database_name}_no_user_{randomness}"
    async with connection_system_engine_no_db.cursor() as cursor:
        await cursor.execute(
            f'CREATE SERVICE ACCOUNT "{sa_account_name}" '
            "WITH DESCRIPTION = 'Ecosytem test with no user'"
        )
        await cursor.execute(f"CALL fb_GENERATESERVICEACCOUNTKEY('{sa_account_name}')")
        # service_account_name, service_account_id, secret
        _, s_id, key = await cursor.fetchone()
        # Currently this is bugged so retrieve id via a query. FIR-28719
        if not s_id:
            await cursor.execute(
                "SELECT service_account_id FROM information_schema.service_accounts "
                f"WHERE service_account_name='{sa_account_name}'"
            )
            s_id = (await cursor.fetchone())[0]
        # Wrap in secret to avoid leaking the key in the logs
        yield s_id, Secret(key)
        await cursor.execute(f'DROP SERVICE ACCOUNT "{sa_account_name}"')


@fixture
async def auth_no_user(
    service_account_no_user: Tuple[str, Secret],
) -> ClientCredentials:
    s_id, s_secret = service_account_no_user
    return ClientCredentials(s_id, s_secret.value)


@fixture
async def mixed_case_db_and_engine(
    connection_system_engine: Connection,
    database_name: str,
    engine_name: str,
) -> Tuple[str, str]:
    test_db_name = f"{database_name}_AMixedCase"
    test_engine_name = f"{engine_name}_AMixedCase"
    system_cursor = connection_system_engine.cursor()
    await system_cursor.execute(f'CREATE DATABASE "{test_db_name}"')
    await system_cursor.execute(f'CREATE ENGINE "{test_engine_name}"')

    yield test_db_name, test_engine_name

    await system_cursor.execute(f'DROP DATABASE "{test_db_name}"')
    await system_cursor.execute(f'STOP ENGINE "{test_engine_name}"')
    await system_cursor.execute(f'DROP ENGINE "{test_engine_name}"')


@fixture
def fb_numeric_paramstyle():
    """Fixture that sets paramstyle to fb_numeric and resets it after the test."""
    original_paramstyle = firebolt.async_db.paramstyle
    firebolt.async_db.paramstyle = "fb_numeric"
    yield
    firebolt.async_db.paramstyle = original_paramstyle
