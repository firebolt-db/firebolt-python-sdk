import json
from re import Pattern, compile
from typing import Callable

from httpx import Request, codes
from pytest import fixture

from firebolt.client.auth import Auth, UsernamePassword
from firebolt.utils.exception import AccountNotFoundError
from firebolt.utils.urls import (
    ACCOUNT_BY_NAME_URL,
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1,
    ACCOUNT_URL,
    AUTH_URL,
    DATABASES_URL,
    ENGINES_URL,
)
from tests.unit.response import Response


@fixture
def user() -> str:
    return "mock_user"


@fixture
def password() -> str:
    return "mock_password"


@fixture
def username_password_auth(user: str, password: str) -> Auth:
    return UsernamePassword(user, password)


# Getting engines


@fixture
def engine_id() -> str:
    return "mock_engine_id"


@fixture
def get_engine_name_by_id_url(
    api_endpoint: str, account_id: str, engine_id: str
) -> str:
    return f"https://{api_endpoint}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id, engine_id=engine_id
    )


@fixture
def get_engines_url(api_endpoint: str) -> str:
    return f"https://{api_endpoint}{ENGINES_URL}"


@fixture
def get_databases_url(api_endpoint: str) -> str:
    return f"https://{api_endpoint}{DATABASES_URL}"


# Getting databases


@fixture
def database_id() -> str:
    return "database_id"


@fixture
def database_by_name_url(api_endpoint: str, account_id: str, db_name: str) -> str:
    return (
        f"https://{api_endpoint}"
        f"{ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)}"
        f"?database_name={db_name}"
    )


@fixture
def database_by_name_callback(account_id: str, database_id: str) -> Callable:
    def do_mock(
        request: Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=codes.OK,
            json={
                "database_id": {
                    "database_id": database_id,
                    "account_id": account_id,
                }
            },
        )

    return do_mock


# db + async db fixtures


@fixture
def auth_url(api_endpoint: str) -> str:
    return f"https://{api_endpoint}{AUTH_URL}"


@fixture
def get_engine_url_by_id_url(api_endpoint: str, account_id: str, engine_id: str) -> str:
    return f"https://{api_endpoint}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id, engine_id=engine_id
    )


@fixture
def get_engine_url_by_id_callback(
    get_engine_url_by_id_url: str, engine_id: str, engine_url: str
) -> Callable:
    def do_mock(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == get_engine_url_by_id_url
        return Response(
            status_code=codes.OK,
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
                    "endpoint": f"https://{engine_url}",
                }
            },
        )

    return do_mock


@fixture
def account_id_url(api_endpoint: str) -> Pattern:
    base = f"https://{api_endpoint}{ACCOUNT_BY_NAME_URL}?account_name="
    default_base = f"https://{api_endpoint}{ACCOUNT_URL}"
    base = base.replace("/", "\\/").replace("?", "\\?")
    default_base = default_base.replace("/", "\\/").replace("?", "\\?")
    return compile(f"(?:{base}.*|{default_base})")


@fixture
def account_id_callback(
    account_id: str,
    account_name: str,
) -> Callable:
    def do_mock(
        request: Request,
        **kwargs,
    ) -> Response:
        if "account_name" not in request.url.params:
            return Response(status_code=codes.OK, json={"account": {"id": account_id}})
        # In this case, an account_name *should* be specified.
        if request.url.params["account_name"] != account_name:
            raise AccountNotFoundError(request.url.params["account_name"])
        return Response(status_code=codes.OK, json={"account_id": account_id})

    return do_mock


@fixture
def engine_by_db_url(api_endpoint: str, account_id: str) -> str:
    return (
        f"https://{api_endpoint}"
        f"{ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1.format(account_id=account_id)}"
    )


@fixture
def check_credentials_callback(user: str, password: str, access_token: str) -> Callable:
    def check_credentials(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = json.loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == user, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == password, "Invalid password"

        return Response(
            status_code=codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    return check_credentials
