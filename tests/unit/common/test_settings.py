import os
from unittest.mock import Mock, patch

from firebolt.client.auth import Auth
from firebolt.common.settings import Settings


def test_settings_happy_path() -> None:
    fields = ("auth", "account_name", "server", "default_region")
    kwargs = {f: (f if f != "auth" else Auth()) for f in fields}
    s = Settings(**kwargs)

    for f in fields:
        field = getattr(s, f)
        assert (
            (field == f) if f != "auth" else isinstance(field, Auth)
        ), f"Invalid settings value {f}"


@patch("firebolt.common.settings.logger")
def test_no_deprecation_warning_with_env(logger_mock: Mock):
    with patch.dict(
        os.environ,
        {
            "FIREBOLT_CLIENT_ID": "client_id",
            "FIREBOLT_CLIENT_SECRET": "client_secret",
            "FIREBOLT_SERVER": "dummy.firebolt.io",
        },
        clear=True,
    ):
        s = Settings(default_region="region")
        logger_mock.warning.assert_not_called()
        assert s.server == "dummy.firebolt.io"
        assert s.auth is not None, "Settings.auth wasn't populated from env variables"
        assert s.auth.client_id == "client_id", "Invalid username in Settings.auth"
        assert (
            s.auth.client_secret == "client_secret"
        ), "Invalid password in Settings.auth"
