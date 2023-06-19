from typing import Any, Dict

from pytest_httpx import HTTPXMock

from firebolt.db.connection import Connection
from firebolt.db.util import is_db_available, is_engine_running


def test_is_db_available(
    connection: Connection,
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
    assert is_db_available(connection, "dummy") == True


def test_is_db_not_available(
    connection: Connection,
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
    assert is_db_available(connection, "dummy") == False


def test_is_engine_running_system(
    system_connection: Connection,
):
    # System engine is always running
    assert is_engine_running(system_connection, "dummy") == True


def test_is_engine_running(
    connection: Connection,
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
    assert is_engine_running(connection, get_engines_url) == True


def test_is_engine_not_running(
    connection: Connection,
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
    assert is_engine_running(connection, get_engines_url) == False
