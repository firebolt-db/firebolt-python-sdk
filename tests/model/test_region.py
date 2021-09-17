from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient
from firebolt.model.region import Region, regions
from tests import paginated


def test_region(
    httpx_mock_auth: HTTPXMock, settings: Settings, mock_regions: List[Region]
):
    httpx_mock_auth.add_response(
        url=f"https://{settings.server}/compute/v1/regions?page.first=5000",
        status_code=httpx.codes.OK,
        json=paginated(mock_regions),
    )
    with FireboltClient(settings=settings):
        assert regions.regions == mock_regions
