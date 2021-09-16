from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient
from firebolt.model.region import Region, regions


def test_region(httpx_mock: HTTPXMock, settings: Settings, mock_regions: List[Region]):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": ""},
    )
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/regions?page.first=5000",
        status_code=httpx.codes.OK,
        json={"edges": [{"node": it.dict()} for it in mock_regions]},
    )
    with FireboltClient(settings=settings):
        assert regions.regions == mock_regions
