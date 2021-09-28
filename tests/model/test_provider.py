from typing import Callable, List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.client import init_firebolt_client
from firebolt.common import Settings
from firebolt.model.provider import Provider
from firebolt.service.provider_service import ProviderService
from tests.util import list_to_paginated_response


def test_provider(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    settings: Settings,
    mock_providers: List[Provider],
):
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/providers?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_providers),
    )
    with init_firebolt_client(settings=settings) as fc:
        assert ProviderService(firebolt_client=fc).providers == mock_providers
