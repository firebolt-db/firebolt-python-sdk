from logging import getLogger
from os import environ

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
SERVICE_ID_ENV = "SERVICE_ID"
SERVICE_SECRET_ENV = "SERVICE_SECRET"


class Secret:
    """
    Class to hold sensitive data in testing. This prevents passwords
    and such to be printed in logs or any other reports.
    More info: https://github.com/pytest-dev/pytest/issues/8613

    NOTE: be careful, assert Secret("") == "" would still print
    on failure
    """

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Secret(********)"

    def __str___(self):
        return "*******"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def rm_settings(api_endpoint, username: str, password: Secret) -> Settings:
    return Settings(
        server=api_endpoint,
        user=username,
        password=password.value,
        default_region="us-east-1",
    )


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
    return Secret(must_env(PASSWORD_ENV))


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def service_id() -> str:
    return must_env(SERVICE_ID_ENV)


@fixture(scope="session")
def service_secret() -> str:
    return Secret(must_env(SERVICE_SECRET_ENV))


@fixture
def service_auth(service_id: str, service_secret: Secret) -> ServiceAccount:
    return ServiceAccount(service_id, service_secret.value)


@fixture(scope="session")
def password_auth(username: str, password: Secret) -> UsernamePassword:
    return UsernamePassword(username, password.value)
