import json
import typing

import httpx
import pytest
from httpx import Response

from firebolt.common.settings import Settings


@pytest.fixture
def test_token(access_token: str) -> str:
    return access_token


@pytest.fixture
def test_token2() -> str:
    return "test_token2"


@pytest.fixture
def test_username(settings: Settings) -> str:
    return settings.user


@pytest.fixture
def test_password(settings: Settings) -> str:
    return settings.password.get_secret_value()


@pytest.fixture
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
