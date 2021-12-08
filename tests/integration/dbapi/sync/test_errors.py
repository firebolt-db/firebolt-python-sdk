from httpx import ConnectError
from pytest import raises

from firebolt.common.exception import (
    AuthenticationError,
    EngineNotRunningError,
    FireboltDatabaseError,
    FireboltEngineError,
    OperationalError,
)
from firebolt.db import Connection, connect


def test_invalid_credentials(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid credentials error"""
    with connect(
        engine_url=engine_url,
        database=database_name,
        username=username + "_",
        password=password + "_",
        api_endpoint=api_endpoint,
        account_name=account_name,
    ) as connection:
        with raises(AuthenticationError) as exc_info:
            connection.cursor().execute("show tables")

        assert str(exc_info.value).startswith(
            "Failed to authenticate"
        ), "Invalid authentication error message"


def test_engine_url_not_exists(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid engine url error"""
    with connect(
        engine_url=engine_url + "_",
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
        account_name=account_name,
    ) as connection:
        with raises(ConnectError):
            connection.cursor().execute("show tables")


def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid engine name error"""
    with raises(FireboltEngineError):
        with connect(
            engine_name=engine_name + "_________",
            database=database_name,
            username=username,
            password=password,
            api_endpoint=api_endpoint,
            account_name=account_name,
        ) as connection:
            connection.cursor().execute("show tables")


def test_engine_stopped(
    stopped_engine_url: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to engine not running error"""
    with raises(EngineNotRunningError):
        with connect(
            engine_url=stopped_engine_url,
            database=database_name,
            username=username,
            password=password,
            api_endpoint=api_endpoint,
            account_name=account_name,
        ) as connection:
            connection.cursor().execute("show tables")


def test_database_not_exists(
    engine_url: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
    account_name: str,
) -> None:
    """Connection properly reacts to invalid database error"""
    new_db_name = database_name + "_"
    with connect(
        engine_url=engine_url,
        database=new_db_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
        account_name=account_name,
    ) as connection:
        with raises(FireboltDatabaseError) as exc_info:
            connection.cursor().execute("show tables")

        assert (
            str(exc_info.value) == f"Database {new_db_name} does not exist"
        ), "Invalid database name error message"


def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to sql execution error"""
    with connection.cursor() as c:
        with raises(OperationalError) as exc_info:
            c.execute("select ]")

        assert str(exc_info.value).startswith(
            "Error executing query"
        ), "Invalid SQL error message"
