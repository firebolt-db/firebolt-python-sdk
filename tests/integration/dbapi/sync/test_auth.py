from time import time

from pytest import mark, raises

from firebolt.db import Connection
from firebolt.utils.exception import AuthenticationError


@mark.skip(reason="flaky, token not updated each time")
def test_refresh_token(connection: Connection) -> None:
    """Auth refreshes token on expiration/invalidation"""
    with connection.cursor() as c:
        # Works fine
        c.execute("show tables")

        # Invalidate the token
        c._client.auth._token += "_"

        # Still works fine
        c.execute("show tables")

        old = c._client.auth.token
        c._client.auth._expires = int(time()) - 1

        # Still works fine
        c.execute("show tables")

        assert c._client.auth.token != old, "Auth didn't update token on expiration"


@mark.parametrize("connection_fixture", ["connection", "service_account_connection"])
def test_credentials_invalidation(connection_fixture: str, request) -> None:
    """Auth raises Authentication Error on credentials invalidation"""
    with request.getfixturevalue(connection_fixture).cursor() as c:
        # Works fine
        c.execute("show tables")

        # Invalidate the token
        c._client.auth._token += "_"
        # Invalidate credentials
        for cred in ("username", "password", "client_id", "client_secret"):
            if hasattr(c._client.auth, cred):
                setattr(c._client.auth, cred, "_")

        with raises(AuthenticationError) as exc_info:
            c.execute("show tables")

        assert str(exc_info.value).startswith(
            "Failed to authenticate"
        ), "Invalid authentication error message"
