from typing import Callable, List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient
from firebolt.model.region import Region, regions
from tests.util import list_to_paginated_response


def test_region(
    httpx_mock: HTTPXMock,
    httpx_mock_auth_callback: Callable,
    settings: Settings,
    mock_regions: List[Region],
):
    httpx_mock.add_callback(httpx_mock_auth_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/regions?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_regions),
    )
    with FireboltClient(settings=settings):
        assert regions.regions == mock_regions
