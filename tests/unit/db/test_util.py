from typing import Any, Dict

from pytest_httpx import HTTPXMock

from firebolt.db.connection import Connection
from firebolt.db.cursor import CursorV2


def test_is_db_available(
    cursor: CursorV2,
    httpx_mock: HTTPXMock,
    query_statistics: Dict[str, Any],
    system_engine_no_db_query_url: str,
):
    # for v2 connection, this is always true
    assert cursor.is_db_available("dummy") == True


def test_is_engine_running_system(
    httpx_mock: HTTPXMock,
    connection: Connection,
):
    cursor = connection.cursor()

    # This is a dummy check, V2 engines are always considered running
    assert cursor.is_engine_running("dummy") is True

    httpx_mock.reset()
