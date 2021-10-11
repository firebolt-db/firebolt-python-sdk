from typing import Callable, List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.region import Region
from firebolt.service.manager import ResourceManager
from tests.util import list_to_paginated_response


def test_region(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    provider_callback: Callable,
    settings: Settings,
    mock_regions: List[Region],
):
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_callback(provider_callback)
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/regions?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_regions),
    )
    manager = ResourceManager(settings=settings)
    assert manager.regions.regions == mock_regions
