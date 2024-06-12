from typing import Callable

from httpx import Request, codes
from pytest import fixture

from firebolt.client.auth import Auth, UsernamePassword
from firebolt.utils.urls import (
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
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
