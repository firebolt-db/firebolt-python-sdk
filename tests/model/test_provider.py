from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient
from firebolt.model.provider import Provider, providers
from tests import paginated


def test_provider(
    httpx_mock_auth: HTTPXMock, settings: Settings, mock_providers: List[Provider]
):
    httpx_mock_auth.add_response(
        url=f"https://{settings.server}/compute/v1/providers?page.first=5000",
        status_code=httpx.codes.OK,
        json=paginated(mock_providers),
    )
    with FireboltClient(settings=settings):
        assert providers.providers == mock_providers
