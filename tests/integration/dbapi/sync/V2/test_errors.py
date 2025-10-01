from pytest import raises

from firebolt.client.auth import ClientCredentials
from firebolt.db import Connection, connect
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    FireboltStructuredError,
)


def test_invalid_account(
    database_name: str,
    invalid_account_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    with raises(AccountNotFoundOrNoAccessError) as exc_info:
        with connect(
            database=database_name,
            auth=auth,
            account_name=invalid_account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")

    assert str(exc_info.value).startswith(
        f"Account '{invalid_account_name}' does not exist"
    ), "Invalid account error message."


def test_account_no_user(
    database_name: str,
    account_name: str,
    auth_no_user: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    with raises((AccountNotFoundOrNoAccessError, FireboltStructuredError)) as exc_info:
        with connect(
            database=database_name,
            auth=auth_no_user,
            account_name=account_name,
            api_endpoint=api_endpoint,
            # Disable cache since for this test we want to make sure
            # the error is raised
            disable_cache=True,
        ) as connection:
            connection.cursor().execute("show tables")

    assert str(exc_info.value).startswith(
        f"Account '{account_name}' does not exist"
    ) or (
        # Caching on the backend may cause this error instead
        f"Database '{database_name}' does not exist or not authorized."
        in str(exc_info.value)
    ), "Invalid account error message."


def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    with raises(FireboltStructuredError) as exc_info:
        with connect(
            account_name=account_name,
            engine_name=engine_name + "_________",
            database=database_name,
            auth=auth,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")

    assert f"Engine '{engine_name}_________' does not exist" in str(
        exc_info.value
    ), "Invalid engine error message."


def test_database_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid database error."""
    new_db_name = database_name + "_"
    with raises(FireboltStructuredError) as exc_info:
        with connect(
            engine_name=engine_name,
            database=new_db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")

    assert f"Database '{new_db_name}' does not exist or not authorized" in str(
        exc_info.value
    ), "Invalid database error message."


def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to sql execution error."""
    with connection.cursor() as c:
        with raises(FireboltStructuredError) as exc_info:
            c.execute("select ]")

        assert "syntax error" in str(exc_info.value), "Invalid SQL error message."
