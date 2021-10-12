from httpx import ConnectError
from pytest import raises

from firebolt.common.exception import (
    AuthenticationError,
    OperationalError,
    ProgrammingError,
)
from firebolt.db import Connection


def test_invalid_credentials(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid credentials error"""
    connection = Connection(
        engine_url, database_name, username + "_", password + "_", api_endpoint
    )
    with raises(AuthenticationError) as exc_info:
        connection.cursor().execute("show tables")

    assert str(exc_info.value).startswith(
        "Failed to authenticate"
    ), "Invalid authentication error message"


def test_engine_not_exists(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid engine url error"""
    connection = Connection(
        engine_url + "_", database_name, username, password, api_endpoint
    )
    with raises(ConnectError):
        connection.cursor().execute("show tables")


def test_database_not_exists(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid database error"""
    new_db_name = database_name + "_"
    connection = Connection(engine_url, new_db_name, username, password, api_endpoint)
    with raises(ProgrammingError) as exc_info:
        connection.cursor().execute("show tables")

    assert (
        str(exc_info.value) == f"Invalid database '{new_db_name}'"
    ), "Invalid database name error message"


def test_sql_error(connection: Connection) -> None:
    with connection.cursor() as c:
        with raises(OperationalError) as exc_info:
            c.execute("select ]")

        assert str(exc_info.value).startswith(
            "Error executing query"
        ), "Invalid SQL error message"
