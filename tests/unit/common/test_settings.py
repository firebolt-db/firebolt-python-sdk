import os
from typing import Tuple
from unittest.mock import Mock, patch

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
    with raises(ValueError) as exc_info:
        Settings(**kwargs)


@patch("firebolt.common.settings.logger")
def test_no_deprecation_warning_with_env(logger_mock: Mock):
    with patch.dict(
        os.environ,
        {"FIREBOLT_USER": "user", "FIREBOLT_PASSWORD": "password"},
        clear=True,
    ):
        s = Settings(server="server", default_region="region")
        logger_mock.warning.assert_not_called()
        assert s.auth is not None, "Settings.auth wasn't populated from env variables"
        assert s.auth.username == "user", "Invalid username in Settings.auth"
        assert s.auth.password == "password", "Invalid password in Settings.auth"
