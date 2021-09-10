import pytest
import respx
from httpx import Response
from pydantic import SecretStr

from firebolt.common.settings import Settings


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
def mocked_api(server, access_token, account_id):
    with respx.mock(
        base_url=f"https://{server}", assert_all_called=False
    ) as respx_mock:
        auth_route = respx_mock.post("/auth/v1/login", name="auth")
        auth_route.return_value = Response(200, json={"access_token": access_token})

        account_id_route = respx_mock.get("/iam/v2/account", name="account_id")
        account_id_route.return_value = Response(
            200, json={"account": {"id": account_id}}
        )
        yield respx_mock


@pytest.fixture
def settings(server):
    return Settings(
        server=server,
        user="email@domain.com",
        password=SecretStr("*****"),
        default_region="us-east-1",
    )
