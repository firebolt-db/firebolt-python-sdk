from typing import Callable

from pytest_httpx import HTTPXMock

from firebolt.common.settings import Settings
from firebolt.service.manager import ResourceManager


def test_rm_credentials(
    httpx_mock: HTTPXMock,
    check_token_callback: Callable,
    check_credentials_callback: Callable,
    settings: Settings,
    auth_url: str,
    account_id_url: str,
    account_id_callback: Callable,
    provider_callback: Callable,
    provider_url: str,
    region_callback: Callable,
    region_url: str,
    access_token: str,
) -> None:
    """Creadentials, that are passed to rm are processed properly."""
    url = "https://url"

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(check_token_callback, url=url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)

    rm = ResourceManager(settings)
    rm.client.get(url)

    token_settings = Settings(
        access_token=access_token,
        server=settings.server,
        default_region=settings.default_region,
    )

    rm = ResourceManager(token_settings)
    rm.client.get(url)
