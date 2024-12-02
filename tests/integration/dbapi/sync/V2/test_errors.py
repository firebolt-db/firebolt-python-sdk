from pytest import mark, raises

from firebolt.client.auth import ClientCredentials
from firebolt.db import Connection, connect
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    FireboltDatabaseError,
    FireboltStructuredError,
)


def test_invalid_account(
    database_name: str,
    invalid_account_name: str,
    auth: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    with raises(AccountNotFoundOrNoAccessError):
        with connect(
            database=database_name,
            auth=auth,
            account_name=invalid_account_name,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")


def test_account_no_user(
    database_name: str,
    account_name: str,
    auth_no_user: ClientCredentials,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid account error."""
    with raises(AccountNotFoundOrNoAccessError):
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


def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    with raises(FireboltStructuredError):
        with connect(
            account_name=account_name,
            engine_name=engine_name + "_________",
            database=database_name,
            auth=auth,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")


@mark.skip(reason="Behaviour is different in prod vs dev")
def test_database_not_exists(
    engine_url: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid database error."""
    new_db_name = database_name + "_"
    with connect(
        account_name=account_name,
        engine_url=engine_url,
        database=new_db_name,
        auth=auth,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(FireboltDatabaseError):
            connection.cursor().execute("show tables")


def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to sql execution error."""
    with connection.cursor() as c:
        with raises(FireboltStructuredError):
            c.execute("select ]")
