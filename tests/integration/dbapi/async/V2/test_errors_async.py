from pytest import raises

from firebolt.async_db import Connection, connect
from firebolt.client.auth import ClientCredentials
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    FireboltStructuredError,
)


async def test_invalid_account(
    database_name: str,
    invalid_account_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    with raises(AccountNotFoundOrNoAccessError) as exc_info:
        async with await connect(
            database=database_name,
            auth=auth,
            account_name=invalid_account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")

    assert str(exc_info.value).startswith(
        f"Account '{invalid_account_name}' does not exist"
    ), "Invalid account error message."


async def test_account_no_user(
    database_name: str,
    account_name: str,
    auth_no_user: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to account that doesn't have
    a user attached to it."""

    with raises(AccountNotFoundOrNoAccessError) as exc_info:
        async with await connect(
            database=database_name,
            auth=auth_no_user,
            account_name=account_name,
            api_endpoint=api_endpoint,
            # Disable cache since for this test we want to make sure
            # the error is raised
            disable_cache=True,
        ) as connection:
            await connection.cursor().execute("show tables")

    assert f"'{account_name}' does not exist" in str(
        exc_info.value
    ), "Invalid account error message."


async def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    with raises(FireboltStructuredError) as exc_info:
        async with await connect(
            engine_name=engine_name + "_________",
            database=database_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")

    assert f"Engine '{engine_name}_________' does not exist" in str(
        exc_info.value
    ), "Invalid engine error message."


async def test_database_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid database error."""
    new_db_name = database_name + "_"
    with raises(FireboltStructuredError) as exc_info:
        async with await connect(
            engine_name=engine_name,
            database=new_db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")

    assert f"Database '{new_db_name}' does not exist or not authorized" in str(
        exc_info.value
    ), "Invalid database error message."


async def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to SQL execution error."""
    async with connection.cursor() as c:
        with raises(FireboltStructuredError) as exc_info:
            await c.execute("select ]")

        assert "syntax error" in str(exc_info.value), "Invalid SQL error message."
