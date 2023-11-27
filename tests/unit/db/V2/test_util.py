from typing import Any, Dict

from pytest_httpx import HTTPXMock

from firebolt.db.connection import Connection
from firebolt.db.cursor import CursorV2


def test_is_db_available(
    cursor: CursorV2,
    httpx_mock: HTTPXMock,
    query_statistics: Dict[str, Any],
    system_engine_query_url: str,
):
    httpx_mock.add_response(
        url=system_engine_query_url,
        method="POST",
        json={
            "rows": "1",
            "data": ["my_db"],
            "meta": [],
            "statistics": query_statistics,
        },
    )
    assert cursor.is_db_available("dummy") == True


def test_is_db_not_available(
    cursor: CursorV2,
    httpx_mock: HTTPXMock,
    system_engine_query_url: str,
    query_statistics: Dict[str, Any],
):
    httpx_mock.add_response(
        url=system_engine_query_url,
        method="POST",
        json={
            "rows": "0",
            "data": [],
            "meta": [],
            "statistics": query_statistics,
        },
    )
    assert cursor.is_db_available("dummy") == False


def test_is_engine_running_system(
    httpx_mock: HTTPXMock,
    system_connection: Connection,
):
    cursor = system_connection.cursor()
    # System engine is always running
    assert cursor.is_engine_running("dummy") == True
    # We didn't resolve account id since since we run no query
    # We need to skip the mocked endpoint
    httpx_mock.reset(False)

    # We haven't used account id endpoint since we didn't run any query, ignoring it
    httpx_mock.reset(False)


def test_is_engine_running(
    cursor: CursorV2,
    httpx_mock: HTTPXMock,
    system_engine_query_url: str,
    query_statistics: Dict[str, Any],
    get_engines_url: str,
):
    httpx_mock.add_response(
        url=system_engine_query_url,
        method="POST",
        json={
            "rows": "1",
            "data": [[get_engines_url, "my_db", "Running"]],
            "meta": [
                {"name": "url", "type": "text"},
                {"name": "attached_to", "type": "text"},
                {"name": "status", "type": "text"},
            ],
            "statistics": query_statistics,
        },
    )
    assert cursor.is_engine_running(get_engines_url) == True


def test_is_engine_not_running(
    cursor: CursorV2,
    httpx_mock: HTTPXMock,
    system_engine_query_url: str,
    query_statistics: Dict[str, Any],
    get_engines_url: str,
):
    httpx_mock.add_response(
        url=system_engine_query_url,
        method="POST",
        json={
            "rows": "1",
            "data": [[get_engines_url, "my_db", "Stopped"]],
            "meta": [
                {"name": "url", "type": "text"},
                {"name": "attached_to", "type": "text"},
                {"name": "status", "type": "text"},
            ],
            "statistics": query_statistics,
        },
    )
    assert cursor.is_engine_running(get_engines_url) == False
