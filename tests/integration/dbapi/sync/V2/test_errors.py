from pytest import mark, raises

from firebolt.client.auth import ClientCredentials
from firebolt.db import Connection, connect
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    EngineNotRunningError,
    FireboltDatabaseError,
    FireboltEngineError,
    OperationalError,
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
    with raises(AccountNotFoundOrNoAccessError) as exc_info:
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
    ), "Invalid account error message."


def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
    account_version: int,
) -> None:
    """Connection properly reacts to invalid engine name error."""
    error_cls = OperationalError if account_version == 2 else FireboltEngineError
    with raises(error_cls):
        with connect(
            account_name=account_name,
            engine_name=engine_name + "_________",
            database=database_name,
            auth=auth,
            api_endpoint=api_endpoint,
        ) as connection:
            connection.cursor().execute("show tables")


@mark.account_v1
def test_engine_stopped(
    stopped_engine_name: str,
    database_name: str,
    auth: ClientCredentials,
    account_name: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to engine not running error."""
    with raises(EngineNotRunningError):
        with connect(
            account_name=account_name,
            engine_name=stopped_engine_name,
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
        with raises(FireboltDatabaseError) as exc_info:
            connection.cursor().execute("show tables")

        assert (
            str(exc_info.value)
            == f"Engine {engine_name} is attached to {database_name} instead of {new_db_name}"
        ), "Invalid database name error message"


def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to sql execution error."""
    with connection.cursor() as c:
        with raises(OperationalError) as exc_info:
            c.execute("select ]")

        assert str(exc_info.value).startswith(
            "Error executing query"
        ), "Invalid SQL error message"
