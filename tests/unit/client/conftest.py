import json
import typing

import httpx
from httpx import Response
from pytest import fixture

from firebolt.common.settings import Settings


@fixture
def test_token(access_token: str) -> str:
    return access_token


@fixture
def test_token2() -> str:
    return "test_token2"


@fixture
def test_username(settings: Settings) -> str:
    return settings.user


@fixture
def test_password(settings: Settings) -> str:
    return settings.password


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
