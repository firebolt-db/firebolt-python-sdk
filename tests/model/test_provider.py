from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import init_firebolt_client
from firebolt.model.provider import Provider, providers


def test_provider(
    httpx_mock: HTTPXMock, settings: Settings, mock_providers: List[Provider]
):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": "", "expiry": 2 ** 32},
    )
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/providers?page.first=5000",
        status_code=httpx.codes.OK,
        json={"edges": [{"node": it.dict()} for it in mock_providers]},
    )
    with init_firebolt_client(settings=settings):
        assert providers.providers == mock_providers
