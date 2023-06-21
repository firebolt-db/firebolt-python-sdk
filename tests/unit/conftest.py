from re import Pattern, compile
from typing import Callable

import httpx
from httpx import Request, Response
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import fixture

from firebolt.client.auth import Auth, ClientCredentials
from firebolt.utils.exception import (
    AccountNotFoundError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from firebolt.utils.urls import (
    ACCOUNT_BY_NAME_URL,
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    AUTH_SERVICE_ACCOUNT_URL,
    DATABASES_URL,
    ENGINES_URL,
)
from tests.unit.db_conftest import *  # noqa


# Register nofakefs mark
def pytest_configure(config):
    config.addinivalue_line("markers", "nofakefs: don't use fakefs fixture")


@fixture(autouse=True)
def global_fake_fs(request) -> None:
    if "nofakefs" in request.keywords:
        yield
    else:
        with Patcher(additional_skip_names=["logger", "allure-pytest"]):
            yield


@fixture
def client_id() -> str:
    return "client_id"


@fixture
def client_secret() -> str:
    return "client_secret"


@fixture
def server() -> str:
    return "api-dev.mock.firebolt.io"


@fixture
def auth_server() -> str:
    return "id.mock.firebolt.io"


@fixture
def account_id() -> str:
    return "mock_account_id"


@fixture
def account_name() -> str:
    return "mock_account_name"


@fixture
def access_token() -> str:
    return "mock_access_token"


@fixture
def access_token_2() -> str:
    return "mock_access_token_2"


@fixture
def auth(client_id: str, client_secret: str) -> Auth:
    return ClientCredentials(client_id, client_secret)


@fixture
def auth_callback(auth_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == auth_url
        return Response(
            status_code=httpx.codes.OK,
            json={"access_token": "", "expires_in": 2**32},
        )

    return do_mock


@fixture
def auth_url(auth_server: str) -> str:
    return f"https://{auth_server}{AUTH_SERVICE_ACCOUNT_URL}"


@fixture
def db_name() -> str:
    return "database"


@fixture
def account_id_url(server: str, account_name: str) -> Pattern:
    account_name_re = r"[^\\\\]*"
    base = f"https://{server}{ACCOUNT_BY_NAME_URL}"
    base = base.replace("/", "\\/").replace("?", "\\?")
    base = base.format(account_name=account_name_re)
    return compile(base)


@fixture
def account_id_callback(
    account_id: str,
    account_name: str,
) -> Callable:
    def do_mock(
        request: Request,
        **kwargs,
    ) -> Response:
        if request.url.path.split("/")[-2] != account_name:
            raise AccountNotFoundError(request.url.path.split("/")[-2])
        return Response(status_code=httpx.codes.OK, json={"id": account_id})

    return do_mock


@fixture
def engine_id() -> str:
    return "mock_engine_id"


@fixture
def engine_endpoint() -> str:
    return "mock_engine_endpoint"


@fixture
def engine_name() -> str:
    return "mock_engine_name"


@fixture
def get_engine_name_by_id_url(server: str, account_id: str, engine_id: str) -> str:
    return f"https://{server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id, engine_id=engine_id
    )


@fixture
def get_engines_url(server: str) -> str:
    return f"https://{server}{ENGINES_URL}"


@fixture
def get_databases_url(server: str) -> str:
    return f"https://{server}{DATABASES_URL}"


@fixture
def database_id() -> str:
    return "database_id"


@fixture
def database_by_name_url(server: str, account_id: str, db_name: str) -> str:
    return (
        f"https://{server}"
        f"{ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)}"
        f"?database_name={db_name}"
    )


@fixture
def database_by_name_callback(account_id: str, database_id: str) -> str:
    def do_mock(
        request: Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json={
                "database_id": {
                    "database_id": database_id,
                    "account_id": account_id,
                }
            },
        )

    return do_mock


@fixture
def db_api_exceptions():
    exceptions = {
        "DatabaseError": DatabaseError,
        "DataError": DataError,
        "Error": Error,
        "IntegrityError": IntegrityError,
        "InterfaceError": InterfaceError,
        "InternalError": InternalError,
        "NotSupportedError": NotSupportedError,
        "OperationalError": OperationalError,
        "ProgrammingError": ProgrammingError,
        "Warning": Warning,
    }
    return exceptions


@fixture
def check_token_callback(access_token: str) -> Callable:
    def check_token(request: Request = None, **kwargs) -> Response:
        prefix = "Bearer "
        assert request, "empty request"
        assert "authorization" in request.headers, "missing authorization header"
        auth = request.headers["authorization"]
        assert auth.startswith(prefix), "invalid authorization header format"
        token = auth[len(prefix) :]
        assert token == access_token, "invalid authorization token"

        return Response(status_code=httpx.codes.OK, headers={"content-length": "0"})

    return check_token


@fixture
def check_credentials_callback(
    client_id: str, client_secret: str, access_token: str
) -> Callable:
    def check_credentials(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = request.read().decode("utf-8")
        assert "client_id" in body, "Missing id"
        assert f"client_id={client_id}" in body, "Invalid id"
        assert "client_secret" in body, "Missing secret"
        assert f"client_secret={client_secret}" in body, "Invalid secret"
        assert "grant_type" in body, "Missing grant_type"
        assert "grant_type=client_credentials" in body, "Invalid grant_type"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    return check_credentials
