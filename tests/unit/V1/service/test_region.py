from re import Pattern
from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.V1.region import Region
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
    account_id_url: Pattern,
    settings: Settings,
    mock_regions: List[Region],
):
    httpx_mock.add_callback(auth_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        provider_callback,
        url=provider_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        region_callback,
        url=region_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    manager = ResourceManager(settings=settings)
    assert manager.regions.regions == mock_regions
