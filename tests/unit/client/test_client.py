from re import Pattern
from typing import Callable

from httpx import codes
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client import Client
from firebolt.client.auth import Auth, ClientCredentials
from firebolt.client.resource_manager_hooks import raise_on_4xx_5xx
from firebolt.utils.token_storage import TokenSecureStorage
from firebolt.utils.urls import AUTH_SERVICE_ACCOUNT_URL
from firebolt.utils.util import fix_url_schema


def test_client_retry(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    access_token: str,
):
    """
    Client retries with new auth token
    if first attempt fails with unauthorized error.
    """
    with Client(account_name=account_name, auth=auth) as client:

        # auth get token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": access_token},
        )

        # client request failed authorization
        httpx_mock.add_response(
            status_code=codes.UNAUTHORIZED,
        )

        # auth get another token
        httpx_mock.add_response(
            status_code=codes.OK,
            json={"expires_in": 2**30, "access_token": access_token},
        )

        # client request success this time
        httpx_mock.add_response(
            status_code=codes.OK,
        )

        assert (
            client.get("https://url").status_code == codes.OK
        ), "request failed with firebolt client"


def test_client_different_auths(
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    check_token_callback: Callable,
    auth: Auth,
    account_name: str,
    auth_server: str,
    server: str,
):
    """
    Client properly handles such auth types:
    - Auth
    - None
    All other types should raise TypeError.
    """

    httpx_mock.add_callback(
        check_credentials_callback,
        url=f"https://{auth_server}{AUTH_SERVICE_ACCOUNT_URL}",
    )

    httpx_mock.add_callback(check_token_callback, url="https://url")

    Client(account_name=account_name, auth=auth, api_endpoint=server).get("https://url")

    with raises(TypeError) as excinfo:
        Client(account_name=account_name, auth=lambda r: r).get("https://url")

    assert str(excinfo.value).startswith(
        'Invalid "auth" argument'
    ), "invalid auth validation error message"


def test_client_account_id(
    httpx_mock: HTTPXMock,
    auth: Auth,
    account_name: str,
    account_id: str,
    account_id_url: Pattern,
    account_id_callback: Callable,
    auth_url: str,
    auth_callback: Callable,
    server: str,
):
    httpx_mock.add_callback(account_id_callback, url=account_id_url)
    httpx_mock.add_callback(auth_callback, url=auth_url)

    with Client(
        account_name=account_name,
        auth=auth,
        base_url=fix_url_schema(server),
        api_endpoint=server,
    ) as c:
        assert c.account_id == account_id, "Invalid account id returned"


# FIR-14945
def test_refresh_with_hooks(
    fs: FakeFilesystem,
    httpx_mock: HTTPXMock,
    account_name: str,
    client_id: str,
    client_secret: str,
    access_token: str,
) -> None:
    """
    When hooks are used, the invalid token, fetched from cache, is refreshed
    """

    tss = TokenSecureStorage(client_id, client_secret)
    tss.cache_token(access_token, 2**32)

    client = Client(
        account_name=account_name,
        auth=ClientCredentials(client_id, client_secret),
        event_hooks={
            "response": [raise_on_4xx_5xx],
        },
    )

    # client request failed authorization
    httpx_mock.add_response(
        status_code=codes.UNAUTHORIZED,
    )

    # auth get another token
    httpx_mock.add_response(
        status_code=codes.OK,
        json={"expires_in": 2**30, "access_token": access_token},
    )

    # client request success this time
    httpx_mock.add_response(
        status_code=codes.OK,
    )

    assert (
        client.get("https://url").status_code == codes.OK
    ), "request failed with firebolt client"
