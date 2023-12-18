from pytest import raises

from firebolt.async_db import Connection, connect
from firebolt.client.auth import ClientCredentials
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    EngineNotRunningError,
    FireboltEngineError,
    InterfaceError,
    OperationalError,
)


async def test_invalid_account(
    database_name: str,
    engine_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    account_name = "--"
    with raises(AccountNotFoundOrNoAccessError) as exc_info:
        async with await connect(
            database=database_name,
            engine_name=engine_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")

        assert str(exc_info.value).startswith(
            f'Account "{account_name}" does not exist'
        ), "Invalid account error message."


async def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    with raises(FireboltEngineError):
        async with await connect(
            engine_name=engine_name + "_________",
            database=database_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")


async def test_engine_stopped(
    stopped_engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    with raises(EngineNotRunningError):
        async with await connect(
            engine_name=stopped_engine_name,
            database=database_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")


async def test_database_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid database error."""
    new_db_name = database_name + "_"
    with raises(InterfaceError) as exc_info:
        async with await connect(
            engine_name=engine_name,
            database=new_db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")

    assert (
        str(exc_info.value)
        == f"Engine {engine_name} is attached to {database_name} instead of {new_db_name}"
    ), "Invalid database name error message."


async def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to SQL execution error."""
    with connection.cursor() as c:
        with raises(OperationalError) as exc_info:
            await c.execute("select ]")

        assert str(exc_info.value).startswith(
            "Error executing query"
        ), "Invalid SQL error message."
