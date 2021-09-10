import pytest
import respx
from httpx import Response
from pydantic import SecretStr

from firebolt.common.settings import Settings
from firebolt.model import FireboltBaseModel
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


def paginated(items: list[FireboltBaseModel]) -> dict:
    """
    Wrap a list in "edge" and "nodes" to mimic how
    the Firebolt API returns paginated reponses.
    """
    return {"edges": [{"node": i.dict()} for i in items]}


@pytest.fixture
def mocked_api(
    server, access_token, account_id, mock_providers, mock_regions, mock_instance_types
):
    with respx.mock(
        base_url=f"https://{server}", assert_all_called=False
    ) as respx_mock:
        auth_route = respx_mock.post("/auth/v1/login", name="auth")
        auth_route.return_value = Response(200, json={"access_token": access_token})

        account_id_route = respx_mock.get("/iam/v2/account", name="account_id")
        account_id_route.return_value = Response(
            200, json={"account": {"id": account_id}}
        )

        providers_route = respx_mock.get("/compute/v1/providers", name="providers")
        providers_route.return_value = Response(200, json=paginated(mock_providers))

        regions_route = respx_mock.get("/compute/v1/regions", name="regions")
        regions_route.return_value = Response(200, json=paginated(mock_regions))

        instance_types_route = respx_mock.get(
            "/compute/v1/instanceTypes", name="instance_types"
        )
        instance_types_route.return_value = Response(
            200, json=paginated(mock_instance_types)
        )
        yield respx_mock


@pytest.fixture
def settings(server) -> Settings:
    return Settings(
        server=server,
        user="email@domain.com",
        password=SecretStr("*****"),
        default_region="us-east-1",
    )
