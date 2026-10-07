from pytest import fixture, mark, raises
from pytest_mock import MockerFixture

from firebolt.client.auth import ClientCredentials
from firebolt.db import Connection
from firebolt.utils.exception import AuthenticationError, AuthorizationError
from tests.integration.conftest import Secret

pytestmark = mark.parametrize("connection_factory", ["remote"], indirect=True)


@fixture
def auth(service_id: str, service_secret: Secret) -> ClientCredentials:
    # These tests mutate credentials; never reuse the session auth or its cache.
    return ClientCredentials(service_id, service_secret.value, use_token_cache=False)


@mark.parametrize("invalidation", ["expired", "invalid"])
def test_refresh_token(
    connection: Connection, mocker: MockerFixture, invalidation: str
) -> None:
    with connection.cursor() as c:
        c.execute("SELECT 1")
        assert c.fetchone() == [1]

        auth = c._client.auth
        refresh = mocker.spy(auth, "get_new_token_generator")
        if invalidation == "expired":
            auth._expires = 0
        else:
            auth._token = "invalid-token"

        c.execute("SELECT 1")
        assert c.fetchone() == [1]
        refresh.assert_called_once()
        assert auth.token
        assert not auth.expired


def test_credentials_invalidation(
    connection: Connection, mocker: MockerFixture
) -> None:
    with connection.cursor() as c:
        c.execute("SELECT 1")
        assert c.fetchone() == [1]

        auth = c._client.auth
        refresh = mocker.spy(auth, "get_new_token_generator")
        auth._token = "invalid-token"
        auth.client_secret = "_"

        with raises((AuthenticationError, AuthorizationError)):
            c.execute("SELECT 1")
        refresh.assert_called_once()
