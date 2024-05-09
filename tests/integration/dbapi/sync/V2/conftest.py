import random
import string
from random import choice
from typing import Tuple

from pytest import fixture

from firebolt.client.auth.base import Auth
from firebolt.client.auth.client_credentials import ClientCredentials
from firebolt.db import Connection, connect
from tests.integration.conftest import Secret


@fixture
def connection(
    engine_name: str,
    database_name: str,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        engine_name=engine_name,
        database=database_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


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


@fixture(scope="session")
def connection_system_engine_v2(
    auth: Auth,
    account_name_v2: str,
    api_endpoint: str,
) -> Connection:
    with connect(
        auth=auth,
        account_name=account_name_v2,
        api_endpoint=api_endpoint,
    ) as connection:
        yield connection


@fixture(scope="session")
def engine_v2(
    connection_system_engine_v2: Connection,
    engine_name: str,
) -> str:
    # randomize the db name to avoid conflicts
    suffix = "".join(choice("0123456789") for _ in range(2))
    engine_name = f"{engine_name}{suffix}_sync"
    cursor = connection_system_engine_v2.cursor()
    cursor.execute(f"CREATE ENGINE IF NOT EXISTS {engine_name}")
    cursor.execute(f"START ENGINE {engine_name}")
    yield engine_name
    cursor.execute(f"STOP ENGINE {engine_name}")
    cursor.execute(f"DROP ENGINE IF EXISTS {engine_name}")


@fixture(scope="session")
def setup_v2_db(connection_system_engine_v2, use_db_name):
    use_db_name = f"{use_db_name}_sync"
    with connection_system_engine_v2.cursor() as cursor:
        # randomize the db name to avoid conflicts
        suffix = "".join(choice("0123456789") for _ in range(2))
        cursor.execute(f"CREATE DATABASE {use_db_name}{suffix}")
        yield f"{use_db_name}{suffix}"
        cursor.execute(f"DROP DATABASE {use_db_name}{suffix}")


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
def service_account_no_user(
    connection_system_engine_no_db: Connection,
    database_name: str,
) -> Tuple[str, Secret]:
    # function-level fixture so we need to make sa name is unique
    randomness = "".join(random.choices(string.ascii_letters + string.digits, k=2))
    sa_account_name = f"{database_name}_sa_no_user_{randomness}"
    with connection_system_engine_no_db.cursor() as cursor:
        cursor.execute(
            f'CREATE SERVICE ACCOUNT "{sa_account_name}" '
            "WITH DESCRIPTION = 'Ecosytem test with no user'"
        )
        cursor.execute(f"CALL fb_GENERATESERVICEACCOUNTKEY('{sa_account_name}')")
        # service_account_name, service_account_id, secret
        _, s_id, key = cursor.fetchone()
        # Currently this is bugged so retrieve id via a query. FIR-28719
        if not s_id:
            cursor.execute(
                "SELECT service_account_id FROM information_schema.service_accounts "
                f"WHERE service_account_name='{sa_account_name}'"
            )
            s_id = cursor.fetchone()[0]
        # Wrap in secret to avoid leaking the key in the logs
        yield s_id, Secret(key)
        cursor.execute(f"DROP SERVICE ACCOUNT {sa_account_name}")


@fixture
def auth_no_user(
    service_account_no_user: Tuple[str, Secret],
) -> ClientCredentials:
    s_id, s_secret = service_account_no_user
    return ClientCredentials(s_id, s_secret.value)
