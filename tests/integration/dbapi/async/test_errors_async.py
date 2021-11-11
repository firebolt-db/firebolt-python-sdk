from httpx import ConnectError
from pytest import mark, raises

from firebolt.async_db import Connection, connect
from firebolt.common.exception import (
    AuthenticationError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)


@mark.asyncio
async def test_invalid_credentials(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid credentials error"""
    async with await connect(
        engine_url=engine_url,
        database=database_name,
        username=username + "_",
        password=password + "_",
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(AuthenticationError) as exc_info:
            await connection.cursor().execute("show tables")

        assert str(exc_info.value).startswith(
            "Failed to authenticate"
        ), "Invalid authentication error message"


@mark.asyncio
async def test_engine_url_not_exists(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid engine url error"""
    async with await connect(
        engine_url=engine_url + "_",
        database=database_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(ConnectError):
            await connection.cursor().execute("show tables")


@mark.asyncio
async def test_engine_name_not_exists(
    engine_name: str,
    database_name: str,
    username: str,
    password: str,
    api_endpoint: str,
) -> None:
    """Connection properly reacts to invalid engine name error"""
    with raises(InterfaceError):
        async with await connect(
            engine_name=engine_name + "_________",
            database=database_name,
            username=username,
            password=password,
            api_endpoint=api_endpoint,
        ) as connection:
            await connection.cursor().execute("show tables")


@mark.asyncio
async def test_database_not_exists(
    engine_url: str, database_name: str, username: str, password: str, api_endpoint: str
) -> None:
    """Connection properly reacts to invalid database error"""
    new_db_name = database_name + "_"
    async with await connect(
        engine_url=engine_url,
        database=new_db_name,
        username=username,
        password=password,
        api_endpoint=api_endpoint,
    ) as connection:
        with raises(ProgrammingError) as exc_info:
            await connection.cursor().execute("show tables")

        assert (
            str(exc_info.value) == f"Invalid database '{new_db_name}'"
        ), "Invalid database name error message"


@mark.asyncio
async def test_sql_error(connection: Connection) -> None:
    """Connection properly reacts to sql execution error"""
    with connection.cursor() as c:
        with raises(OperationalError) as exc_info:
            await c.execute("select ]")

        assert str(exc_info.value).startswith(
            "Error executing query"
        ), "Invalid SQL error message"
