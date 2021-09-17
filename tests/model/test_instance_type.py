from typing import List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient
from firebolt.model.instance_type import InstanceType, instance_types
from tests import paginated


def test_instance_type(
    httpx_mock_auth: HTTPXMock,
    settings: Settings,
    mock_instance_types: List[InstanceType],
):
    httpx_mock_auth.add_response(
        url=f"https://{settings.server}/compute/v1/instanceTypes?page.first=5000",
        status_code=httpx.codes.OK,
        json=paginated(mock_instance_types),
    )
    with FireboltClient(settings=settings):
        assert instance_types.instance_types == mock_instance_types
