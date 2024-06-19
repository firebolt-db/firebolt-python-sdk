from logging import getLogger
from os import environ
from time import time
from typing import Optional

from pytest import fixture, mark

from firebolt.client.auth import ClientCredentials
from firebolt.client.auth.username_password import UsernamePassword

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "ENGINE_NAME"
STOPPED_ENGINE_NAME_ENV = "STOPPED_ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"
SERVICE_ID_ENV = "SERVICE_ID"
SERVICE_SECRET_ENV = "SERVICE_SECRET"
USER_NAME_ENV = "USER_NAME"
PASSWORD_ENV = "PASSWORD"
ENGINE_URL_ENV = "ENGINE_URL"
STOPPED_ENGINE_URL_ENV = "STOPPED_ENGINE_URL"

# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
# Adding slow marker to tests


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        # --runslow isn't given in cli: skip slow tests
        skip_slow = mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


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
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def stopped_engine_name() -> str:
    return must_env(STOPPED_ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def use_db_name(database_name: str):
    return f"{database_name}_use_db_test"


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def invalid_account_name(account_name: str) -> str:
    return f"{account_name}--"


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def service_id() -> str:
    return must_env(SERVICE_ID_ENV)


@fixture(scope="session")
def service_secret() -> Secret:
    return Secret(must_env(SERVICE_SECRET_ENV))


@fixture(scope="session")
def auth(service_id: str, service_secret: Secret) -> ClientCredentials:
    return ClientCredentials(service_id, service_secret.value)


@fixture(scope="session")
def username() -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="session")
def password() -> str:
    return Secret(must_env(PASSWORD_ENV))


@fixture(scope="session")
def password_auth(username: str, password: Secret) -> UsernamePassword:
    return UsernamePassword(username, password.value)


@fixture(scope="session")
def engine_url() -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="session")
def stopped_engine_url() -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


@fixture(scope="function")
def minimal_time():
    limit: Optional[float] = None

    def setter(value):
        nonlocal limit
        limit = value

    start = time()
    yield setter
    end = time()
    if limit is not None:
        assert (
            end - start >= limit
        ), f"Test took {end - start} seconds, less than {limit} seconds"
