from httpx import AsyncClient, Request, codes
from pytest import mark
from pytest_httpx import HTTPXMock

from firebolt.client import Auth
from tests.unit.util import async_execute_generator_requests


@mark.asyncio
async def test_auth_refresh_on_expiration(
    httpx_mock: HTTPXMock,
    test_token: str,
    test_token2: str,
):
    """Auth refreshes the token on expiration."""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token2},
    )

    auth = Auth("user", "password")
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == test_token, "invalid access token"
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == test_token2, "expired access token was not updated"


@mark.asyncio
async def test_auth_uses_same_token_if_valid(
    httpx_mock: HTTPXMock,
    test_token: str,
    test_token2: str,
):
    """Auth refreshes the token on expiration"""

    # To get token for the first time
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2 ** 32, "access_token": test_token},
    )

    # Request
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    # To refresh token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2 ** 32, "access_token": test_token2},
    )

    # Request
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    auth = Auth("user", "password")
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == test_token, "invalid access token"
    await async_execute_generator_requests(
        auth.async_auth_flow(Request("GET", "https://host"))
    )
    assert auth.token == test_token, "shoud not update token until it expires"
    httpx_mock.reset(False)


@mark.asyncio
async def test_auth_adds_header(
    httpx_mock: HTTPXMock,
    test_token: str,
):
    """Auth adds required authentication headers to httpx.Request."""
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 0, "access_token": test_token},
    )

    auth = Auth("user", "password")
    async with AsyncClient() as client:
        flow = auth.async_auth_flow(Request("get", ""))
        request = await flow.__anext__()
        response = await client.send(request)
        request = await flow.asend(response)

    assert "authorization" in request.headers, "missing authorization header"
    assert (
        request.headers["authorization"] == f"Bearer {test_token}"
    ), "missing authorization header"
