from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import init_firebolt_client
from firebolt.model.instance_type import InstanceType, instance_types


def test_instance_type(
    httpx_mock: HTTPXMock, settings: Settings, mock_instance_types: List[InstanceType]
):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": "", "expires_in": 2 ** 32},
    )
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/instanceTypes?page.first=5000",
        status_code=httpx.codes.OK,
        json={"edges": [{"node": it.dict()} for it in mock_instance_types]},
    )
    with init_firebolt_client(settings=settings):
        assert instance_types.instance_types == mock_instance_types
