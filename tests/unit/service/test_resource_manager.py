from typing import Callable

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Token, UsernamePassword
from firebolt.common.settings import Settings
from firebolt.service.manager import ResourceManager
from firebolt.utils.token_storage import TokenSecureStorage


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

    auth_username_password_settings = Settings(
        auth=UsernamePassword(settings.user, settings.password.get_secret_value()),
        server=settings.server,
        default_region=settings.default_region,
    )

    rm = ResourceManager(auth_username_password_settings)
    rm.client.get(url)

    auth_token_settings = Settings(
        auth=Token(access_token),
        server=settings.server,
        default_region=settings.default_region,
    )

    rm = ResourceManager(auth_token_settings)
    rm.client.get(url)


@mark.nofakefs
def test_rm_token_cache(
    httpx_mock: HTTPXMock,
    check_token_callback: Callable,
    check_credentials_callback: Callable,
    settings: Settings,
    auth_url: str,
    account_id_url: str,
    account_id_callback: Callable,
    provider_callback: Callable,
    provider_url: str,
    access_token: str,
) -> None:
    """Creadentials, that are passed to rm are processed properly."""
    url = "https://url"

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(check_token_callback, url=url)
    httpx_mock.add_callback(provider_callback, url=provider_url)
    httpx_mock.add_callback(account_id_callback, url=account_id_url)

    with Patcher():
        local_settings = Settings(
            user=settings.user,
            password=settings.password.get_secret_value(),
            server=settings.server,
            default_region=settings.default_region,
            use_token_cache=True,
        )
        rm = ResourceManager(local_settings)
        rm.client.get(url)

        ts = TokenSecureStorage(settings.user, settings.password.get_secret_value())
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        local_settings = Settings(
            user=settings.user,
            password=settings.password.get_secret_value(),
            server=settings.server,
            default_region=settings.default_region,
            use_token_cache=False,
        )
        rm = ResourceManager(local_settings)
        rm.client.get(url)

        ts = TokenSecureStorage(settings.user, settings.password.get_secret_value())
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"
