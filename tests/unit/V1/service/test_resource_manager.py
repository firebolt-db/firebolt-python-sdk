from re import Pattern
from typing import Callable

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth, Token, UsernamePassword
from firebolt.common.settings import Settings
from firebolt.service.manager import ResourceManager
from firebolt.utils.cache import _firebolt_cache
from firebolt.utils.exception import AccountNotFoundError
from tests.unit.test_cache_helpers import get_cached_token


def test_rm_credentials(
    httpx_mock: HTTPXMock,
    check_token_callback: Callable,
    check_credentials_callback: Callable,
    settings: Settings,
    user: str,
    password: str,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    access_token: str,
) -> None:
    """Credentials, that are passed to rm are processed properly."""
    url = "https://url"

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        provider_callback,
        url=provider_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        check_token_callback,
        url=url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    rm = ResourceManager(settings)
    rm._client.get(url)

    auth_username_password_settings = Settings(
        auth=UsernamePassword(user, password),
        server=settings.server,
        default_region=settings.default_region,
    )

    rm = ResourceManager(auth_username_password_settings)
    rm._client.get(url)

    auth_token_settings = Settings(
        auth=Token(access_token),
        server=settings.server,
        default_region=settings.default_region,
    )

    rm = ResourceManager(auth_token_settings)
    rm._client.get(url)


@mark.nofakefs
def test_rm_token_cache(
    httpx_mock: HTTPXMock,
    check_token_callback: Callable,
    check_credentials_callback: Callable,
    settings: Settings,
    user: str,
    password: str,
    auth_url: str,
    provider_callback: Callable,
    provider_url: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    access_token: str,
    enable_cache: Callable,
) -> None:
    """Credentials, that are passed to rm are processed properly."""
    url = "https://url"

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        provider_callback,
        url=provider_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        check_token_callback,
        url=url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    with Patcher():
        local_settings = Settings(
            auth=UsernamePassword(user, password, use_token_cache=True),
            server=settings.server,
            default_region=settings.default_region,
        )
        rm = ResourceManager(local_settings)
        rm._client.get(url)

        # Verify token was cached using the new cache system
        cached_token = get_cached_token(user, password)
        assert cached_token == access_token, "Invalid token value cached"

    _firebolt_cache.clear()
    # Do the same, but with use_token_cache=False
    with Patcher():
        local_settings = Settings(
            auth=UsernamePassword(user, password, use_token_cache=False),
            server=settings.server,
            default_region=settings.default_region,
        )
        rm = ResourceManager(local_settings)
        rm._client.get(url)

        # Verify token was not cached when caching is disabled
        cached_token = get_cached_token(user, password)
        assert cached_token is None, "Token is cached even though caching is disabled"


def test_rm_invalid_account_name(
    httpx_mock: HTTPXMock,
    auth: Auth,
    settings: Settings,
    check_credentials_callback: Callable,
    auth_url: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
) -> None:
    """Resource manager raises an error on invalid account name."""
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        account_id_callback,
        url=account_id_url,
        is_reusable=True,
    )

    local_settings = Settings(
        auth=auth,
        account_name="invalid",
        server=settings.server,
        default_region=settings.default_region,
    )

    with raises(AccountNotFoundError):
        ResourceManager(local_settings)
