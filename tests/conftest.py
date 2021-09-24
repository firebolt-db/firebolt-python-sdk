from typing import Callable

import httpx
import pytest
from pydantic import SecretStr
from pytest_httpx import to_response
from pytest_httpx._httpx_internals import Response

from firebolt.common.settings import Settings
from firebolt.model.instance_type import InstanceType, InstanceTypeKey
from firebolt.model.provider import Provider
from firebolt.model.region import Region, RegionKey


@pytest.fixture
def server() -> str:
    return "api.mock.firebolt.io"


@pytest.fixture
def account_id() -> str:
    return "mock_account_id"


@pytest.fixture
def access_token() -> str:
    return "mock_access_token"


@pytest.fixture
def provider() -> Provider:
    return Provider(
        provider_id="mock_provider_id",
        name="mock_provider_name",
    )


@pytest.fixture
def mock_providers(provider) -> list[Provider]:
    return [provider]


@pytest.fixture
def region_1(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_1",
        ),
        name="mock_region_1",
    )


@pytest.fixture
def region_2(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_2",
        ),
        name="mock_region_2",
    )


@pytest.fixture
def mock_regions(region_1, region_2) -> list[Region]:
    return [region_1, region_2]


@pytest.fixture
def instance_type_1(provider, region_1) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_1.key.region_id,
            instance_type_id="instance_type_id_1",
        ),
        name="instance_type_1",
    )


@pytest.fixture
def instance_type_2(provider, region_2) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_2.key.region_id,
            instance_type_id="instance_type_id_2",
        ),
        name="instance_type_2",
    )


@pytest.fixture
def mock_instance_types(instance_type_1, instance_type_2) -> list[InstanceType]:
    return [instance_type_1, instance_type_2]


@pytest.fixture
def settings(server) -> Settings:
    return Settings(
        server=server,
        user="email@domain.com",
        password=SecretStr("*****"),
    )


@pytest.fixture
def auth_callback(auth_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == auth_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"access_token": "", "expires_in": 2 ** 32},
        )

    return do_mock


@pytest.fixture
def auth_url(settings: Settings) -> str:
    return f"https://{settings.server}/auth/v1/login"


@pytest.fixture
def db_name() -> str:
    return "database"
