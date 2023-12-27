from pytest import fixture

from firebolt.client.auth import UsernamePassword
from firebolt.service.manager import ResourceManager


@fixture
def resource_manager(
    password_auth: UsernamePassword, api_endpoint: str
) -> ResourceManager:
    return ResourceManager(
        auth=password_auth,
        api_endpoint=api_endpoint,
        default_region="us-east-1",
    )
