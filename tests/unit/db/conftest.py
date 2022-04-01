from pytest import fixture

from firebolt.common.settings import Settings
from firebolt.db import Connection, Cursor, connect


@fixture
def connection(settings: Settings, db_name: str) -> Connection:
    with connect(
        engine_url=settings.server,
        database=db_name,
        username="u",
        password="p",
        api_endpoint=settings.server,
    ) as connection:
        yield connection


@fixture
def cursor(connection: Connection) -> Cursor:
    return connection.cursor()
