from json import loads
from re import Pattern, compile

import httpx
from pytest import fixture

from firebolt.async_db import Connection, Cursor, connect
from firebolt.client import AsyncClientV1 as Client
from firebolt.client.auth.base import Auth
from firebolt.client.auth.username_password import UsernamePassword
from firebolt.utils.exception import AccountNotFoundError
from firebolt.utils.urls import (
    ACCOUNT_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1,
    ACCOUNT_URL,
    AUTH_URL,
)
from tests.unit.db_conftest import *  # noqa


@fixture
async def connection(
    server: str, db_name: str, username_password_auth: UsernamePassword
) -> Connection:
    async with (
        await connect(
            engine_url=server,
            database=db_name,
            auth=username_password_auth,
            api_endpoint=server,
        )
    ) as connection:
        yield connection


@fixture
async def cursor(connection: Connection) -> Cursor:
    return connection.cursor()


@fixture
def auth_url(server: str) -> str:
    return f"https://{server}{AUTH_URL}"


@fixture
def get_engine_url_by_id_url(server: str, account_id: str, engine_id: str) -> str:
    return f"https://{server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id, engine_id=engine_id
    )


@fixture
def get_engine_url_by_id_callback(
    get_engine_url_by_id_url: str, engine_id: str, server: str
) -> Callable:
    def do_mock(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == get_engine_url_by_id_url
        return Response(
            status_code=codes.OK,
            json={
                "engine": {
                    "name": "name",
                    "compute_region_id": {
                        "provider_id": "provider",
                        "region_id": "region",
                    },
                    "settings": {
                        "preset": "",
                        "auto_stop_delay_duration": "1s",
                        "minimum_logging_level": "",
                        "is_read_only": False,
                        "warm_up": "",
                    },
                    "endpoint": f"https://{server}",
                }
            },
        )

    return do_mock


@fixture
def account_id_url(server: str) -> Pattern:
    base = f"https://{server}{ACCOUNT_BY_NAME_URL}?account_name="
    default_base = f"https://{server}{ACCOUNT_URL}"
    base = base.replace("/", "\\/").replace("?", "\\?")
    default_base = default_base.replace("/", "\\/").replace("?", "\\?")
    return compile(f"(?:{base}.*|{default_base})")


@fixture
def account_id_callback(
    account_id: str,
    account_name: str,
) -> Callable:
    def do_mock(
        request: Request,
        **kwargs,
    ) -> Response:
        if "account_name" not in request.url.params:
            return Response(
                status_code=httpx.codes.OK, json={"account": {"id": account_id}}
            )
        # In this case, an account_name *should* be specified.
        if request.url.params["account_name"] != account_name:
            raise AccountNotFoundError(request.url.params["account_name"])
        return Response(status_code=httpx.codes.OK, json={"account_id": account_id})

    return do_mock


@fixture
def engine_by_db_url(server: str, account_id: str) -> str:
    return (
        f"https://{server}"
        f"{ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1.format(account_id=account_id)}"
    )


@fixture
def client(
    server: str,
    account_name: str,
    auth: Auth,
) -> Client:
    return Client(
        account_name=account_name,
        auth=auth,
        api_endpoint=server,
    )


@fixture
def check_credentials_callback(user: str, password: str, access_token: str) -> Callable:
    def check_credentials(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == user, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == password, "Invalid password"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    return check_credentials
