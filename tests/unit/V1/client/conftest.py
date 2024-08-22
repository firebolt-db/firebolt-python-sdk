import json
import typing
from re import Pattern, compile

import httpx
from httpx import Request
from pytest import fixture

from firebolt.utils.exception import AccountNotFoundError
from firebolt.utils.urls import ACCOUNT_BY_NAME_URL_V1, ACCOUNT_URL, AUTH_URL
from tests.unit.response import Response


@fixture
def test_token(access_token: str) -> str:
    return access_token


@fixture
def test_token2() -> str:
    return "test_token2"


@fixture
def test_username(user: str) -> str:
    return user


@fixture
def test_password(password: str) -> str:
    return password


@fixture
def mock_service_id() -> str:
    return "mock_service_id"


@fixture
def mock_service_secret() -> str:
    return "my_secret"


@fixture
def check_credentials_callback(
    test_username: str, test_password: str, test_token: str
) -> typing.Callable:
    def check_credentials(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = json.loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == test_username, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == test_password, "Invalid password"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": test_token},
        )

    return check_credentials


@fixture
def check_service_credentials_callback(
    mock_service_id: str, mock_service_secret: str, test_token: str
) -> typing.Callable:
    def check_credentials(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = request.read().decode("utf-8")
        assert "client_id" in body, "Missing id"
        assert f"client_id={mock_service_id}" in body, "Invalid id"
        assert "client_secret" in body, "Missing secret"
        assert f"client_secret={mock_service_secret}" in body, "Invalid secret"
        assert "grant_type" in body, "Missing grant_type"
        assert "grant_type=client_credentials" in body, "Invalid grant_type"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": test_token},
        )

    return check_credentials


@fixture
def account_id_url(api_endpoint: str) -> Pattern:
    base = f"https://{api_endpoint}{ACCOUNT_BY_NAME_URL_V1}?account_name="
    default_base = f"https://{api_endpoint}{ACCOUNT_URL}"
    base = base.replace("/", "\\/").replace("?", "\\?")
    default_base = default_base.replace("/", "\\/").replace("?", "\\?")
    return compile(f"(?:{base}.*|{default_base})")


@fixture
def account_id_callback(
    account_id: str,
    account_name: str,
) -> typing.Callable:
    def do_mock(
        request: Request,
        **kwargs,
    ) -> Response:
        if "account_name" not in request.url.params:
            return Response(
                status_code=httpx.codes.OK,
                json={"account": {"id": account_id, "name": account_name}},
            )
        # In this case, an account_name *should* be specified.
        if request.url.params["account_name"] != account_name:
            raise AccountNotFoundError(request.url.params["account_name"])
        return Response(status_code=httpx.codes.OK, json={"account_id": account_id})

    return do_mock


@fixture
def auth_url(api_endpoint: str) -> str:
    return f"https://{api_endpoint}{AUTH_URL}"
