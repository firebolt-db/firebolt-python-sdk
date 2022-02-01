from typing import Tuple

from pydantic import ValidationError
from pytest import mark, raises

from firebolt.common.settings import Settings


@mark.parametrize(
    "fields",
    (
        ("user", "password", "account_name", "server", "default_region"),
        ("access_token", "account_name", "server", "default_region"),
    ),
)
def test_settings_happy_path(fields: Tuple[str]) -> None:
    kwargs = {f: f for f in fields}
    s = Settings(**kwargs)

    for f in fields:
        field = getattr(s, f)
        if hasattr(field, "get_secret_value"):
            field = field.get_secret_value()
        assert field == f, f"Invalid settings value {f}"


def test_settings_auth_credentials() -> None:
    creds = ("access_token", "user", "password")
    other = ("server", "default_region")

    kwargs = {f: f for f in other}
    with raises(ValidationError) as exc_info:
        Settings(**kwargs)

    err = exc_info.value
    assert (
        len(err.errors()) > 0
        and err.errors()[0]["msg"] == "Provide either user/password or access_token"
    ), "Invalid error message"

    kwargs = {f: f for f in creds + other}
    with raises(ValidationError) as exc_info:
        Settings(**kwargs)

    err = exc_info.value
    assert (
        len(err.errors()) > 0
        and err.errors()[0]["msg"]
        == "Provide only one of user/password or access_token"
    ), "Invalid error message"
