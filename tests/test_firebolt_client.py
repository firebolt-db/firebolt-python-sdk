import httpx
from pytest_httpx import HTTPXMock

from firebolt.common import Settings
from firebolt.firebolt_client import FireboltClient


def test_settings(settings: Settings, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": ""},
    )
    with FireboltClient(settings=settings) as fc:
        assert fc.settings == settings


def test_auth(
    httpx_mock: HTTPXMock,
    settings: Settings,
    access_token: str,
):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": access_token},
    )
    with FireboltClient(settings=settings) as fc:
        assert fc.access_token == access_token


def test_account_id(httpx_mock: HTTPXMock, settings: Settings, account_id: str):
    httpx_mock.add_response(
        url=f"https://{settings.server}/auth/v1/login",
        status_code=httpx.codes.OK,
        json={"access_token": ""},
    )
    httpx_mock.add_response(
        url=f"https://{settings.server}/iam/v2/account",
        status_code=httpx.codes.OK,
        json={"account": {"id": account_id}},
    )
    with FireboltClient(settings=settings) as fc:
        assert fc.account_id == account_id
