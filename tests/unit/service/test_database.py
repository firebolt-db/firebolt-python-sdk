from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.service.manager import ResourceManager


def test_database_create(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    settings: Settings,
    account_id_callback: Callable,
    account_id_url: str,
    databases_callback: Callable,
    databases_url: str,
    db_name: str,
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(databases_callback, url=databases_url, method="POST")

    manager = ResourceManager(settings=settings)
    database = manager.databases.create(name=db_name)
    assert database.name == db_name


def test_database_get_by_name(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    settings: Settings,
    account_id_callback: Callable,
    account_id_url: str,
    database_get_by_name_callback: Callable,
    database_get_by_name_url: str,
    database_get_callback: Callable,
    database_get_url: str,
    db_name: str,
):

    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(database_get_by_name_callback, url=database_get_by_name_url)
    httpx_mock.add_callback(database_get_callback, url=database_get_url)

    manager = ResourceManager(settings=settings)
    database = manager.databases.get_by_name(name=db_name)
    assert database.name == db_name
