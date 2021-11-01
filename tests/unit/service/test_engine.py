from typing import Callable, List

import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.model.engine import Engine
from firebolt.model.instance_type import InstanceType
from firebolt.service.manager import ResourceManager
from tests.unit.util import list_to_paginated_response


def test_engine_create(
    httpx_mock: HTTPXMock,
    auth_callback: Callable,
    provider_callback: Callable,
    settings: Settings,
    mock_instance_types: List[InstanceType],
    mock_regions,
    mock_engine: Engine,
    engine_name: str,
    account_id_url: str,
    account_id_callback: Callable,
):
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_callback(provider_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/instanceTypes?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_instance_types),
    )
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback)
    httpx_mock.add_response(
        url=f"https://{settings.server}/compute/v1/regions?page.first=5000",
        status_code=httpx.codes.OK,
        json=list_to_paginated_response(mock_regions),
    )
    httpx_mock.add_response(
        url=f"https://{settings.server}/core/v1/account/engines",
        status_code=httpx.codes.OK,
        json={"engine": mock_engine.dict()},
        method="POST",
    )

    manager = ResourceManager(settings=settings)
    engine = manager.engines.create(name=engine_name)

    assert engine.name == engine_name
