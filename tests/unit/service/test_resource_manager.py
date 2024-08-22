from typing import Callable

from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import mark
from pytest_httpx import HTTPXMock

from firebolt.client.auth import Auth, ClientCredentials
from firebolt.service.manager import ResourceManager
from firebolt.utils.token_storage import TokenSecureStorage


def test_rm_credentials(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    check_token_callback: Callable,
    mock_system_engine_connection_flow: Callable,
) -> None:
    """Credentials, that are passed to rm are processed properly."""
    url = "https://url"

    mock_system_engine_connection_flow()
    httpx_mock.add_callback(check_token_callback, url=url)

    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )
    rm._client.get(url)


@mark.nofakefs
def test_rm_token_cache(
    httpx_mock: HTTPXMock,
    check_token_callback: Callable,
    auth: Auth,
    api_endpoint: str,
    account_name: str,
    access_token: str,
    mock_system_engine_connection_flow: Callable,
) -> None:
    """Credentials, that are passed to rm are cached properly."""
    url = "https://url"

    mock_system_engine_connection_flow()
    httpx_mock.add_callback(check_token_callback, url=url)

    with Patcher():
        rm = ResourceManager(
            auth=ClientCredentials(
                auth.client_id, auth.client_secret, use_token_cache=True
            ),
            account_name=account_name,
            api_endpoint=api_endpoint,
        )
        rm._client.get(url)

        ts = TokenSecureStorage(auth.client_id, auth.client_secret)
        assert ts.get_cached_token() == access_token, "Invalid token value cached"

    # Do the same, but with use_token_cache=False
    with Patcher():
        rm = ResourceManager(
            auth=ClientCredentials(
                auth.client_id, auth.client_secret, use_token_cache=False
            ),
            account_name=account_name,
            api_endpoint=api_endpoint,
        )
        rm._client.get(url)

        ts = TokenSecureStorage(auth.client_id, auth.client_secret)
        assert (
            ts.get_cached_token() is None
        ), "Token is cached even though caching is disabled"
