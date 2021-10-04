from typing import Callable, List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager
from tests.util import list_to_paginated_response


def test_instance_type(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    settings: Settings,
    mock_instance_types: List[InstanceType],
):
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/instanceTypes?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_instance_types),
    )
    manager = ResourceManager(settings=settings)
    assert manager.instance_types.instance_types == mock_instance_types
