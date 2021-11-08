from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.region import Region
from firebolt.service.manager import ResourceManager


def test_region(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    account_id_callback: Callable,
    account_id_url: str,
    settings: Settings,
    mock_regions: List[Region],
):
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)
    httpx_mock.add_callback(region_callback, url=region_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)

    manager = ResourceManager(settings=settings)
    assert manager.regions.regions == mock_regions
