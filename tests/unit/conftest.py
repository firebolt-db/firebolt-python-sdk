from json import loads
from typing import Callable, List

import httpx
from httpx import Response
from pydantic import SecretStr
from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import fixture

from firebolt.common.settings import Settings
from firebolt.model.provider import Provider
from firebolt.model.region import Region, RegionKey
from firebolt.utils.exception import (
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
    ACCOUNT_ENGINE_URL_BY_DATABASE_NAME,
    ACCOUNT_URL,
    AUTH_URL,
    DATABASES_URL,
    ENGINES_URL,
    PROVIDERS_URL,
)
from tests.unit.db_conftest import *  # noqa
from tests.unit.util import list_to_paginated_response


# Register nofakefs mark
def pytest_configure(config):
    config.addinivalue_line("markers", "nofakefs: don't use fakefs fixture")


@fixture(autouse=True)
def global_fake_fs(request) -> None:
    if "nofakefs" in request.keywords:
        yield
    else:
        with Patcher():
            yield


@fixture
def server() -> str:
    return "api.mock.firebolt.io"


@fixture
def account_id() -> str:
    return "mock_account_id"


@fixture
def access_token() -> str:
    return "mock_access_token"


@fixture
def provider() -> Provider:
    return Provider(
        provider_id="mock_provider_id",
        name="mock_provider_name",
    )


@fixture
def mock_providers(provider) -> List[Provider]:
    return [provider]


@fixture
def region_1(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_1",
        ),
        name="mock_region_1",
    )


@fixture
def region_2(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_2",
        ),
        name="mock_region_2",
    )


@fixture
def mock_regions(region_1, region_2) -> List[Region]:
    return [region_1, region_2]


@fixture
def settings(server, region_1) -> Settings:
    return Settings(
        server=server,
        user="email@domain.com",
        password=SecretStr("*****"),
        default_region=region_1.name,
        account_name=None,
    )


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
def auth_url(settings: Settings) -> str:
    return f"https://{settings.server}{AUTH_URL}"


@fixture
def db_name() -> str:
    return "database"


@fixture
def db_description() -> str:
    return "database description"


@fixture
def account_id_url(settings: Settings) -> str:
    if not settings.account_name:  # if None or ''
        return f"https://{settings.server}{ACCOUNT_URL}"
    else:
        return (
            f"https://{settings.server}{ACCOUNT_BY_NAME_URL}"
            f"?account_name={settings.account_name}"
        )


@fixture
def account_id_callback(
    account_id: str, account_id_url: str, settings: Settings
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == account_id_url
        if account_id_url.endswith(ACCOUNT_URL):  # account_name shouldn't be specified.
            return Response(
                status_code=httpx.codes.OK, json={"account": {"id": account_id}}
            )
        # In this case, an account_name *should* be specified.
        return Response(status_code=httpx.codes.OK, json={"account_id": account_id})

    return do_mock


@fixture
def engine_id() -> str:
    return "engine_id"


@fixture
def get_engine_url(settings: Settings, account_id: str, engine_id: str) -> str:
    return f"https://{settings.server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id, engine_id=engine_id
    )


@fixture
def get_engine_callback(
    get_engine_url: str, engine_id: str, settings: Settings
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == get_engine_url
        return Response(
            status_code=httpx.codes.OK,
            json={
                "engine": {
                    "name": "name",
                    "compute_region_id": {
                        "provider_id": "provider",
                        "region_id": "region",
                    },
                    "settings": {
                        "preset": "",
                        "auto_stop_delay_duration": "1s",
                        "minimum_logging_level": "",
                        "is_read_only": False,
                        "warm_up": "",
                    },
                    "endpoint": f"https://{settings.server}",
                }
            },
        )

    return do_mock


@fixture
def get_providers_url(settings: Settings, account_id: str, engine_id: str) -> str:
    return f"https://{settings.server}{PROVIDERS_URL}"


@fixture
def get_providers_callback(get_providers_url: str, provider: Provider) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == get_providers_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([provider]),
        )

    return do_mock


@fixture
def get_engines_url(settings: Settings) -> str:
    return f"https://{settings.server}{ENGINES_URL}"


@fixture
def get_databases_url(settings: Settings) -> str:
    return f"https://{settings.server}{DATABASES_URL}"


@fixture
def database_id() -> str:
    return "database_id"


@fixture
def database_by_name_url(settings: Settings, account_id: str, db_name: str) -> str:
    return (
        f"https://{settings.server}"
        f"{ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)}"
        f"?database_name={db_name}"
    )


@fixture
def database_by_name_callback(account_id: str, database_id: str) -> str:
    def do_mock(
        request: httpx.Request = None,
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
def engine_by_db_url(settings: Settings, account_id: str) -> str:
    return (
        f"https://{settings.server}"
        f"{ACCOUNT_ENGINE_URL_BY_DATABASE_NAME.format(account_id=account_id)}"
    )


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
    def check_token(request: httpx.Request = None, **kwargs) -> Response:
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
def check_credentials_callback(settings: Settings, access_token: str) -> Callable:
    def check_credentials(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == settings.user, "Invalid username"
        assert "password" in body, "Missing password"
        assert (
            body["password"] == settings.password.get_secret_value()
        ), "Invalid password"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    return check_credentials
