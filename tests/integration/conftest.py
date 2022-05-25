from logging import getLogger
from os import environ

from _pytest.fixtures import SubRequest
from pytest import fixture

from firebolt.client.auth import ServiceAccount, UsernamePassword
from firebolt.service.manager import Settings

LOGGER = getLogger(__name__)

ENGINE_URL_ENV = "ENGINE_URL"
ENGINE_NAME_ENV = "ENGINE_NAME"
STOPPED_ENGINE_URL_ENV = "STOPPED_ENGINE_URL"
STOPPED_ENGINE_NAME_ENV = "STOPPED_ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
USER_NAME_ENV = "USER_NAME"
PASSWORD_ENV = "PASSWORD"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"
CLIENT_ID_ENV = "CLIENT_ID"
CLIENT_SECRET_ENV = "CLIENT_SECRET"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_url() -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="session")
def stopped_engine_url() -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def stopped_engine_name() -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def username() -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def client_id() -> str:
    return must_env(CLIENT_ID_ENV)


@fixture(scope="session")
def client_secret() -> str:
    return must_env(CLIENT_SECRET_ENV)


@fixture
def username_password_auth(
    username: str,
    password: str,
) -> str:
    return UsernamePassword(username, password)


@fixture
def service_account_auth(client_id: str, client_secret: str) -> str:
    return ServiceAccount(client_id, client_secret)


@fixture(params=["username_password", "service_account"])
def any_auth(
    username_password_auth: UsernamePassword,
    service_account_auth: ServiceAccount,
    request: SubRequest,
) -> str:
    auths = {
        "username_password": username_password_auth,
        "service_account": service_account_auth,
    }
    return auths[request.param]


@fixture
def rm_settings(api_endpoint, any_auth) -> Settings:
    return Settings(
        server=api_endpoint,
        auth=auth,
        default_region="us-east-1",
    )
