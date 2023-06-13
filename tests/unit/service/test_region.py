from typing import Callable, List

from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.region import Region
from firebolt.service.manager import ResourceManager


def test_region(
    httpx_mock: HTTPXMock,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    settings: Settings,
    mock_regions: List[Region],
    mock_system_engine_connection_flow: Callable,
):
    mock_system_engine_connection_flow()
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(region_callback, url=region_url)

    manager = ResourceManager(settings=settings)
    assert manager.regions.regions == mock_regions
