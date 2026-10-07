import importlib.util
import logging
from pathlib import Path

import pytest

SECRET_ENV_NAMES = (
    "SERVICE_ID",
    "SERVICE_SECRET",
    "USER_NAME",
    "PASSWORD",
    # A credential under a name the allowlist has never heard of.
    "CLIENT_SECRET",
)

# Built at runtime rather than written out as literal name/value pairs, so this
# file carries nothing for a secret scanner to flag.
SECRET_ENVS = {name: f"{name.lower()}-must-not-be-logged" for name in SECRET_ENV_NAMES}

LOGGED_ENVS = {
    "ENGINE_NAME": "engine-ok",
    "DATABASE_NAME": "database-ok",
    "ACCOUNT_NAME": "account-ok",
}


def _load_integration_conftest():
    path = Path(__file__).resolve().parents[1] / "integration" / "conftest.py"
    spec = importlib.util.spec_from_file_location(
        "integration_conftest_under_test", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _capture(logger):
    """Collect records off `logger` directly.

    Deliberately avoids the `caplog` fixture: loading the integration conftest
    leaves the pytest logging plugin's per-item stash unset, so `caplog` raises
    a KeyError when this module runs as part of the full suite.
    """
    records = []

    class Collector(logging.Handler):
        def emit(self, record):
            records.append(self.format(record))

    handler = Collector()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return records, handler


@pytest.mark.nofakefs
def test_must_env_logs_only_allowlisted_env_vars(monkeypatch):
    integration_conftest = _load_integration_conftest()

    for name, value in {**SECRET_ENVS, **LOGGED_ENVS}.items():
        monkeypatch.setenv(name, value)

    logger = integration_conftest.LOGGER
    records, handler = _capture(logger)
    original_level, original_propagate = logger.level, logger.propagate
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        for name, value in {**SECRET_ENVS, **LOGGED_ENVS}.items():
            assert integration_conftest.must_env(name) == value
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)
        logger.propagate = original_propagate

    logged = "\n".join(records)

    for name, value in SECRET_ENVS.items():
        assert value not in logged, f"{name} value was logged"
        assert name not in logged, f"{name} name was logged"

    for name, value in LOGGED_ENVS.items():
        assert f"{name}: {value}" in logged
