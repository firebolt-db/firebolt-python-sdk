from time import time

from pytest import mark, raises

from firebolt.async_db import Connection
from firebolt.utils.exception import AuthenticationError


@mark.skip(reason="flaky, token not updated each time")
async def test_refresh_token(connection: Connection) -> None:
    """Auth refreshes token on expiration/invalidation"""
    with connection.cursor() as c:
        # Works fine
        await c.execute("show tables")

        # Invalidate the token
        c._client.auth._token += "_"

        # Still works fine
        await c.execute("show tables")

        old = c._client.auth.token
        c._client.auth._expires = int(time()) - 1

        # Still works fine.
        await c.execute("show tables")

        assert c._client.auth.token != old, "Auth didn't update token on expiration."


async def test_credentials_invalidation(
    connection: Connection, service_account_connection: Connection
) -> None:
    """Auth raises authentication error on credentials invalidation"""
    # Can't pytest.parametrize it due to nested event loop error
    for conn in [connection, service_account_connection]:
        with conn.cursor() as c:
            # Works fine
            await c.execute("show tables")

            # Invalidate the token
            c._client.auth._token += "_"
            # Invalidate credentials
            for cred in ("username", "password", "client_id", "client_secret"):
                if hasattr(c._client.auth, cred):
                    setattr(c._client.auth, cred, "_")

            with raises(AuthenticationError) as exc_info:
                await c.execute("show tables")

            assert str(exc_info.value).startswith(
                "Failed to authenticate"
            ), "Invalid authentication error message"
