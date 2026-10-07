from pytest import fixture, mark, raises
from pytest_mock import MockerFixture

from firebolt.async_db import Connection
from firebolt.client.auth import ClientCredentials
from firebolt.utils.exception import AuthenticationError, AuthorizationError
from tests.integration.conftest import Secret

pytestmark = mark.parametrize("connection_factory", ["remote"], indirect=True)


@fixture
def auth(service_id: str, service_secret: Secret) -> ClientCredentials:
    # These tests mutate credentials; never reuse the session auth or its cache.
    return ClientCredentials(service_id, service_secret.value, use_token_cache=False)


@mark.parametrize("invalidation", ["expired", "invalid"])
async def test_refresh_token(
    connection: Connection, mocker: MockerFixture, invalidation: str
) -> None:
    async with connection.cursor() as c:
        await c.execute("SELECT 1")
        assert await c.fetchone() == [1]

        auth = c._client.auth
        refresh = mocker.spy(auth, "get_new_token_generator")
        if invalidation == "expired":
            auth._expires = 0
        else:
            auth._token = "invalid-token"

        await c.execute("SELECT 1")
        assert await c.fetchone() == [1]
        refresh.assert_called_once()
        assert auth.token
        assert not auth.expired


async def test_credentials_invalidation(
    connection: Connection, mocker: MockerFixture
) -> None:
    async with connection.cursor() as c:
        await c.execute("SELECT 1")
        assert await c.fetchone() == [1]

        auth = c._client.auth
        refresh = mocker.spy(auth, "get_new_token_generator")
        auth._token = "invalid-token"
        auth.client_secret = "_"

        with raises((AuthenticationError, AuthorizationError)):
            await c.execute("SELECT 1")
        refresh.assert_called_once()
