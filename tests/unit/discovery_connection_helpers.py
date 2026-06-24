from typing import Callable

from httpx import Request, codes
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth
from firebolt.utils.exception import ConfigurationError, InterfaceError
from tests.unit.response import Response

DISCOVERY_HOST = "localhost:3473"
DISCOVERY_URL = f"http://{DISCOVERY_HOST}/.well-known/firebolt"
DISCOVERY_SETTINGS = {"custom_setting": "custom_value"}


def mock_discovery_connection_flow(
    httpx_mock: HTTPXMock,
    db_name: str,
    engine_name: str,
    query_callback: Callable,
) -> None:
    httpx_mock.add_response(
        method="GET",
        url=DISCOVERY_URL,
        json={"engineUrl": f"{DISCOVERY_HOST}/?discovered_param=value"},
    )

    def query_with_discovery_params(request: Request, **kwargs) -> Response:
        params = dict(request.url.params)
        assert "authorization" not in request.headers
        assert params["database"] == db_name
        assert params["engine"] == engine_name
        assert params["custom_setting"] == DISCOVERY_SETTINGS["custom_setting"]
        assert params["discovered_param"] == "value"
        return query_callback(request, **kwargs)

    httpx_mock.add_callback(query_with_discovery_params, method="POST")


def assert_discovery_validation_errors(connect_call: Callable, auth: Auth) -> None:
    with raises(ConfigurationError, match="account_name"):
        connect_call(account_name="account")
    with raises(ConfigurationError, match="api_endpoint"):
        connect_call(api_endpoint="api.example.com")
    with raises(ConfigurationError, match="engine_url"):
        connect_call(engine_url="engine.example.com")
    with raises(ConfigurationError, match="url"):
        connect_call(url=f"http://{DISCOVERY_HOST}")
    with raises(ConfigurationError, match="auth"):
        connect_call(auth=auth)


def mock_discovery_not_found(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=DISCOVERY_URL,
        status_code=codes.NOT_FOUND,
        text="not found",
    )


def assert_discovery_lookup_error(connect_call: Callable) -> None:
    with raises(InterfaceError, match="Unable to retrieve Firebolt discovery"):
        connect_call()


async def assert_async_discovery_lookup_error(connect_call: Callable) -> None:
    with raises(InterfaceError, match="Unable to retrieve Firebolt discovery"):
        await connect_call()
