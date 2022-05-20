from typing import Tuple

from pydantic import ValidationError
from pytest import mark, raises

from firebolt.client.auth import Auth
from firebolt.common.settings import Settings


@mark.parametrize(
    "fields",
    (
        ("user", "password", "account_name", "server", "default_region"),
        ("access_token", "account_name", "server", "default_region"),
        ("auth", "account_name", "server", "default_region"),
    ),
)
def test_settings_happy_path(fields: Tuple[str]) -> None:
    kwargs = {f: (f if f != "auth" else Auth()) for f in fields}
    s = Settings(**kwargs)

    for f in fields:
        field = getattr(s, f)
        if hasattr(field, "get_secret_value"):
            field = field.get_secret_value()
        assert (
            (field == f) if f != "auth" else isinstance(field, Auth)
        ), f"Invalid settings value {f}"


creds_fields = ("access_token", "user", "password")
other_fields = ("server", "default_region")


@mark.parametrize(
    "kwargs",
    (
        {f: f for f in other_fields},
        {f: f for f in creds_fields + other_fields},
        {"auth": Auth(), "access_token": "123", **{f: f for f in other_fields}},
    ),
)
def test_settings_auth_credentials(kwargs) -> None:
    with raises(ValidationError) as exc_info:
        Settings(**kwargs)

    err = exc_info.value
    assert len(err.errors()) > 0
