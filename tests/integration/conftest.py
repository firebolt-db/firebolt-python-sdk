import os
import subprocess
from logging import getLogger
from os import environ, getenv
from time import time
from typing import Optional

from pytest import fixture, mark

from firebolt.client.auth import ClientCredentials
from firebolt.client.auth.firebolt_core import FireboltCore
from firebolt.client.auth.username_password import UsernamePassword
from tests.integration.cluster.compose import ComposeAppManager
from tests.integration.cluster.helm import HelmAppManager

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "ENGINE_NAME"
STOPPED_ENGINE_NAME_ENV = "STOPPED_ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"
SERVICE_ID_ENV = "SERVICE_ID"
SERVICE_SECRET_ENV = "SERVICE_SECRET"
USER_NAME_ENV = "USER_NAME"
PASSWORD_ENV = "PASSWORD"
ENGINE_URL_ENV = "ENGINE_URL"
STOPPED_ENGINE_URL_ENV = "STOPPED_ENGINE_URL"
CORE_URL_ENV = "CORE_URL"

KIND_CLUSTER_NAME = "firebolt-python-sdk"


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-kind", action="store_true", help="Run integration tests against kind"
    )
    parser.addoption(
        "--run-compose",
        action="store_true",
        help="Run integration tests against docker-compose",
    )
    parser.addoption(
        "--run-https",
        action="store_true",
        help="Run integration tests against https endpoint",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line(
        "markers",
        "dedicated_core_cluster: Marker for tests that need a dedicated core cluster installation",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        # --runslow isn't given in cli: skip slow tests
        skip_slow = mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


class Secret:
    """
    Class to hold sensitive data in testing. This prevents passwords
    and such to be printed in logs or any other reports.
    More info: https://github.com/pytest-dev/pytest/issues/8613
    NOTE: be careful, assert Secret("") == "" would still print
    on failure
    """

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Secret(********)"

    def __str___(self):
        return "*******"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="function")
def engine_name(app_setup) -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="function")
def stopped_engine_name(app_setup) -> str:
    return must_env(STOPPED_ENGINE_NAME_ENV)


@fixture(scope="function")
def database_name(app_setup) -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="function")
def use_db_name(app_setup, database_name: str):
    return f"{database_name}_use_db_test"


@fixture(scope="function")
def account_name(app_setup) -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="function")
def invalid_account_name(app_setup, account_name: str) -> str:
    return f"{account_name}--"


@fixture(scope="function")
def api_endpoint(app_setup) -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="function")
def service_id(app_setup) -> str:
    return must_env(SERVICE_ID_ENV)


@fixture(scope="function")
def service_secret(app_setup) -> Secret:
    return Secret(must_env(SERVICE_SECRET_ENV))


@fixture(scope="function")
def auth(app_setup, service_id: str, service_secret: Secret) -> ClientCredentials:
    return ClientCredentials(service_id, service_secret.value)


@fixture(scope="function")
def core_auth(app_setup) -> FireboltCore:
    return FireboltCore()


@fixture(scope="function")
def username(app_setup) -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="function")
def password(app_setup) -> str:
    return Secret(must_env(PASSWORD_ENV))


@fixture(scope="function")
def password_auth(app_setup, username: str, password: Secret) -> UsernamePassword:
    return UsernamePassword(username, password.value)


@fixture(scope="function")
def engine_url(app_setup) -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="function")
def stopped_engine_url(app_setup) -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


@fixture(scope="function")
def core_url(app_setup) -> str:
    return getenv(CORE_URL_ENV, "")


@fixture(scope="function")
def minimal_time():
    limit: Optional[float] = None

    def setter(value):
        nonlocal limit
        limit = value

    start = time()
    yield setter
    end = time()
    if limit is not None:
        assert (
            end - start >= limit
        ), f"Test took {end - start} seconds, less than {limit} seconds"


def pytest_generate_tests(metafunc):
    if "app_setup" in metafunc.fixturenames:
        run_kind = metafunc.config.getoption("--run-kind")
        run_compose = metafunc.config.getoption("--run-compose")
        run_https = metafunc.config.getoption("--run-https")

        if run_kind and run_https:
            raise ValueError(
                "The --run-kind and --run-https arguments are not compatible. HTTPS is only supported with --run-compose."
            )

        backends = []

        if run_compose:
            backends.append("docker-compose")
        if run_kind:
            backends.append("kind")

        # Apply the parametrization
        if backends:
            metafunc.parametrize("app_setup", backends, indirect=True)


@fixture(scope="session")
def kind_cluster(request):
    """
    Creates a kind cluster once per session using the provided kind.yaml.
    Only if the --run-kind flag is provided.
    """
    run_kind = request.config.getoption("--run-kind")

    if not run_kind:
        # If we aren't running kind tests, don't do the heavy setup
        yield None
        return

    # Check if cluster already exists
    result = subprocess.run(["kind", "get", "clusters"], capture_output=True, text=True)
    clusters = result.stdout.splitlines()

    if KIND_CLUSTER_NAME in clusters:
        print(
            f"\n[Kind] Cluster '{KIND_CLUSTER_NAME}' already exists. Skipping creation."
        )
    else:
        print(f"\n[Kind] Creating cluster '{KIND_CLUSTER_NAME}'...")
        subprocess.run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                KIND_CLUSTER_NAME,
                "--config",
                "kind.yaml",
                "--wait",
                "5m",
            ],
            check=True,
        )

    context = f"kind-{KIND_CLUSTER_NAME}"
    yield context

    # Optional: Only delete when running in CI
    if os.getenv("GITHUB_ACTIONS") == "true":
        print(f"\n[Kind] CI detected: Deleting cluster '{KIND_CLUSTER_NAME}'...")
        subprocess.run(
            ["kind", "delete", "cluster", "--name", KIND_CLUSTER_NAME], check=True
        )
    else:
        print(
            f"\n[Kind] Local: Cluster '{KIND_CLUSTER_NAME}' will be kept for next session."
        )


@fixture(scope="session")
def session_helm_install(request, kind_cluster):
    """The fast, shared Kind/Helm deployment."""
    if not request.config.getoption("--run-kind"):
        yield None
        return

    manager = HelmAppManager(kind_cluster)
    data = manager.deploy()
    yield data
    manager.cleanup(data)


@fixture(scope="session")
def session_compose_install(request):
    """The fast, shared Docker Compose deployment."""
    run_kind = request.config.getoption("--run-kind")
    run_compose = request.config.getoption("--run-compose")

    # Only run if --run-compose is explicitly requested
    if not run_compose:
        yield None
        return

    manager = ComposeAppManager()
    data = manager.deploy()
    yield data
    manager.cleanup(data)


@fixture(scope="function")
def dedicated_helm_install(request, kind_cluster):
    """Create a dedicated Kind/Helm installation."""
    marker = request.node.get_closest_marker("dedicated_core_cluster")
    if marker and getattr(request, "param", "docker-compose") == "kind":
        # Prefer marker args if present, else fallback to indirect param
        params = marker.args[0] if marker.args else getattr(request, "param", {})
        manager = HelmAppManager(kind_cluster)
        data = manager.deploy(params=params)
        yield data
        manager.cleanup(data)
    else:
        yield None


@fixture(scope="function")
def dedicated_compose_install(request):
    """Create a dedicated Docker Compose installation."""
    marker = request.node.get_closest_marker("dedicated_core_cluster")
    if marker and getattr(request, "param", "docker-compose") == "docker-compose":
        # Prefer marker args if present, else fallback to indirect param
        params = marker.args[0] if marker.args else getattr(request, "param", {})
        manager = ComposeAppManager()
        data = manager.deploy(params=params)
        yield data
        manager.cleanup(data)
    else:
        yield None


@fixture(scope="function")
def app_setup(
    request,
    session_helm_install,
    session_compose_install,
    dedicated_helm_install,
    dedicated_compose_install,
):
    """
    Dynamically injects the required environment variables from active setup.
    """
    backend = getattr(request, "param", "remote")
    run_https = request.config.getoption("--run-https")

    if backend == "kind":
        active_setup = dedicated_helm_install or session_helm_install
    elif backend == "docker-compose":
        active_setup = dedicated_compose_install or session_compose_install
    else:
        active_setup = None

    if active_setup:
        if active_setup in (dedicated_helm_install, dedicated_compose_install):
            print(f"\n[DEBUG] Using DEDICATED install at {active_setup['url']}")
        else:
            print(f"\n[DEBUG] Using SESSION install at {active_setup['url']}")

    if active_setup:
        url = active_setup["url"]
        ips = active_setup.get("ips", ["127.0.0.1"])
        env_vars = {
            "CORE_URL": url,
            "DATABASE_NAME": "firebolt",
            "ENGINE_NAME": "",
            "STOPPED_ENGINE_NAME": "",
            "API_ENDPOINT": "",
            "ACCOUNT_NAME": "",
            "SERVICE_ID": "",
            "SERVICE_SECRET": "",
            "ENGINE_URL": "",
            "CORE_IPS": ",".join(ips),
        }

        if run_https and backend == "docker-compose":
            # Override url to use https if provided
            nginx_ports = active_setup.get("nginx_ports", [])
            if not nginx_ports:
                raise ValueError("HTTPS is requested, but no nginx ports available.")

            https_ips = [f"https://127.0.0.1:{port}" for port in nginx_ports]
            env_vars["CORE_URL"] = https_ips[0]
            env_vars["CORE_IPS"] = ",".join(https_ips)

            # Set SSL cert file for https requests
            cert_path = active_setup.get("server_cert_path")
            if not cert_path:
                raise ValueError("HTTPS is requested, but no cert path available.")
            env_vars["SSL_CERT_FILE"] = cert_path

        old_env = {k: os.environ.get(k) for k in env_vars}
        os.environ.update(env_vars)

        yield backend

        # Restore env
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    else:
        # Fallback if no setup is active (e.g. external compose or remote)
        yield backend
