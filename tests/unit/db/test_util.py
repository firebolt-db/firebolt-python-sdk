from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common.settings import Settings
from firebolt.db.connection import Connection
from firebolt.db.util import is_db_available, is_engine_running
from firebolt.utils.urls import DATABASES_URL, ENGINES_URL


def test_is_db_available(
    connection: Connection,
    httpx_mock: HTTPXMock,
    settings: Settings,
    auth_url: str,
    auth_callback: Callable,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_response(
        url=f"https://{settings.server}{DATABASES_URL}?filter.name_contains=dummy",
        method="GET",
        json={"edges": ["one"]},
    )
    assert is_db_available(connection, "dummy") == True


def test_is_db_not_available(
    connection: Connection,
    httpx_mock: HTTPXMock,
    settings: Settings,
    auth_url: str,
    auth_callback: Callable,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_response(
        url=f"https://{settings.server}{DATABASES_URL}?filter.name_contains=dummy",
        method="GET",
        json={"edges": []},
    )
    assert is_db_available(connection, "dummy") == False


def test_is_engine_running(
    connection: Connection,
    httpx_mock: HTTPXMock,
    settings: Settings,
    auth_url: str,
    auth_callback: Callable,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_response(
        url=f"https://{settings.server}{ENGINES_URL}?filter.name_contains=my_engine&filter.current_status_eq=ENGINE_STATUS_RUNNING_REVISION_SERVING",
        method="GET",
        json={"edges": ["one"]},
    )
    assert is_engine_running(connection, "https://my-engine.dev.firebolt.io") == True


def test_is_engine_not_running(
    connection: Connection,
    httpx_mock: HTTPXMock,
    settings: Settings,
    auth_url: str,
    auth_callback: Callable,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_response(
        url=f"https://{settings.server}{ENGINES_URL}?filter.name_contains=my_engine&filter.current_status_eq=ENGINE_STATUS_RUNNING_REVISION_SERVING",
        method="GET",
        json={"edges": []},
    )
    assert is_engine_running(connection, "https://my-engine.dev.firebolt.io") == False
