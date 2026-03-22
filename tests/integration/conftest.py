import os
import socket
import subprocess
import uuid
from logging import getLogger
from os import environ, getenv
from time import sleep, time
from typing import Optional

from pytest import fixture, mark

from firebolt.client.auth import ClientCredentials
from firebolt.client.auth.firebolt_core import FireboltCore
from firebolt.client.auth.username_password import UsernamePassword

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

# Variables to configure the Firebolt Core Helm installation
CORE_HELM_CHART_VERSION_ENV = "CORE_HELM_CHART_VERSION"
CORE_DEFAULT_HELM_CHART_VERSION = "0.3.0"
CORE_IMAGE_TAG_ENV = "CORE_IMAGE_TAG"
CORE_PORT = 3473

KIND_CLUSTER_NAME = "firebolt-python-sdk"

# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
# Adding slow marker to tests


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


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line(
        "markers", "kind_only: tests that require a Kubernetes/Kind environment"
    )
    config.addinivalue_line(
        "markers",
        "dedicated_helm_install: Marker for tests that need a dedicated helm release installation",
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
def engine_name(kind_app_setup) -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="function")
def stopped_engine_name(kind_app_setup) -> str:
    return must_env(STOPPED_ENGINE_NAME_ENV)


@fixture(scope="function")
def database_name(kind_app_setup) -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="function")
def use_db_name(kind_app_setup, database_name: str):
    return f"{database_name}_use_db_test"


@fixture(scope="function")
def account_name(kind_app_setup) -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="function")
def invalid_account_name(kind_app_setup, account_name: str) -> str:
    return f"{account_name}--"


@fixture(scope="function")
def api_endpoint(kind_app_setup) -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="function")
def service_id(kind_app_setup) -> str:
    return must_env(SERVICE_ID_ENV)


@fixture(scope="function")
def service_secret(kind_app_setup) -> Secret:
    return Secret(must_env(SERVICE_SECRET_ENV))


@fixture(scope="function")
def auth(kind_app_setup, service_id: str, service_secret: Secret) -> ClientCredentials:
    return ClientCredentials(service_id, service_secret.value)


@fixture(scope="function")
def core_auth(kind_app_setup) -> FireboltCore:
    return FireboltCore()


@fixture(scope="function")
def username(kind_app_setup) -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="function")
def password(kind_app_setup) -> str:
    return Secret(must_env(PASSWORD_ENV))


@fixture(scope="function")
def password_auth(kind_app_setup, username: str, password: Secret) -> UsernamePassword:
    return UsernamePassword(username, password.value)


@fixture(scope="function")
def engine_url(kind_app_setup) -> str:
    return must_env(ENGINE_URL_ENV)


@fixture(scope="function")
def stopped_engine_url(kind_app_setup) -> str:
    return must_env(STOPPED_ENGINE_URL_ENV)


@fixture(scope="function")
def core_url(kind_app_setup) -> str:
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
    if "kind_app_setup" in metafunc.fixturenames:
        run_kind = metafunc.config.getoption("--run-kind")
        run_compose = metafunc.config.getoption("--run-compose")
        is_kind_only = metafunc.definition.get_closest_marker("kind_only") is not None

        # Default behavior: if no flags are provided, we assume --run-compose
        if not run_kind and not run_compose:
            run_compose = True

        backends = []

        # Logic for tests marked with @pytest.mark.kind_only
        if is_kind_only:
            if run_kind:
                backends = ["kind"]
            else:
                # If kind_only but --run-kind wasn't requested, we skip it
                backends = []

        # Logic for standard tests
        else:
            if run_compose:
                backends.append("docker-compose")
            if run_kind:
                backends.append("kind")

        # Apply the parametrization
        if backends:
            metafunc.parametrize("kind_app_setup", backends, indirect=True)
        else:
            # If no backends match (e.g. kind_only test but no --run-kind flag),
            # we parametrize with an empty list which skips the test.
            metafunc.parametrize("kind_app_setup", [], indirect=True)


def get_free_port():
    """Ask the OS for a free ephemeral port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


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


def deploy_app(kind_cluster, helm_values=None):
    """Common logic for both session and function-scoped setups."""
    test_id = (
        f"{os.environ.get('PYTEST_XDIST_WORKER', 'python-sdk')}-{uuid.uuid4().hex[:4]}"
    )
    release, ns = f"core-{test_id}", f"ns-{test_id}"
    local_port = get_free_port()

    # Use CORE_IMAGE_TAG if not provided in helm_values
    if helm_values is None:
        helm_values = {}
    if "image.tag" not in helm_values:
        core_image_tag = getenv(CORE_IMAGE_TAG_ENV)
        if core_image_tag:
            helm_values["image.tag"] = core_image_tag

    set_args = []
    if helm_values:
        for key, value in helm_values.items():
            set_args.extend(["--set", f"{key}={value}"])

    print(f"[Kind] Installing Helm release {release} into namespace {ns}...")
    subprocess.run(
        [
            "helm",
            "install",
            release,
            "oci://ghcr.io/firebolt-db/helm-charts/firebolt-core",
            "--version",
            getenv(CORE_HELM_CHART_VERSION_ENV, CORE_DEFAULT_HELM_CHART_VERSION),
            "-n",
            ns,
            "--create-namespace",
            "--wait",
            "--kube-context",
            kind_cluster,
        ]
        + set_args,
        check=True,
    )

    print(f"[Kind] Waiting for pods in {ns} to be ready...")
    subprocess.run(
        [
            "kubectl",
            "wait",
            "--for=condition=ready",
            "pod",
            "-l",
            "app.kubernetes.io/instance=" + release,
            "--namespace",
            ns,
            "--timeout=120s",
            "--context",
            kind_cluster,
        ],
        check=True,
    )

    pod_names_result = subprocess.run(
        [
            "kubectl",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=" + release,
            "-n",
            ns,
            "-o",
            "jsonpath={.items[*].metadata.name}",
            "--context",
            kind_cluster,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    pod_names = pod_names_result.stdout.split()

    pf_procs = []
    ips_with_ports = []
    for i, pod_name in enumerate(pod_names):
        ip = "127.0.0.1"
        port = local_port + i
        ips_with_ports.append(f"{ip}:{port}")
        print(f"[Kind] Port-forward to pod {pod_name} on {ip}:{port}->{CORE_PORT}...")
        pf_proc = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                "--address",
                ip,
                f"pod/{pod_name}",
                f"{port}:{CORE_PORT}",
                "-n",
                ns,
                "--context",
                kind_cluster,
            ]
        )
        pf_procs.append(pf_proc)

    sleep(1)
    # Wait for port-forward
    for i in range(10):
        try:
            # Check all port-forwards
            all_up = True
            for ip_port in ips_with_ports:
                ip, port = ip_port.split(":")
                try:
                    with socket.create_connection((ip, int(port)), timeout=1):
                        print(f"[Kind] Port-forward on {ip}:{port} is UP")
                except (socket.error, ConnectionRefusedError):
                    all_up = False
                    break
            if all_up:
                break
        except (socket.error, ConnectionRefusedError):
            if i == 9:
                raise RuntimeError(f"Failed to connect to port-forward on {local_port}")
            sleep(1)

    url = f"http://127.0.0.1:{local_port}"

    # Return everything needed for cleanup
    return {
        "url": url,
        "processes": pf_procs,
        "release": release,
        "ns": ns,
        "ips": ips_with_ports,
    }


def cleanup_app(setup_data, kind_cluster):
    """Common teardown logic."""
    for proc in setup_data["processes"]:
        proc.terminate()
    subprocess.run(
        [
            "helm",
            "uninstall",
            setup_data["release"],
            "-n",
            setup_data["ns"],
            "--kube-context",
            kind_cluster,
        ],
        check=True,
    )
    subprocess.run(["kubectl", "delete", "ns", setup_data["ns"]], check=True)


@fixture(scope="session")
def session_helm_install(request, kind_cluster):
    """The fast, shared deployment."""
    if not request.config.getoption("--run-kind"):
        yield None
        return

    data = deploy_app(kind_cluster)
    yield data
    cleanup_app(data, kind_cluster)


@fixture(scope="function")
def dedicated_helm_install(request, kind_cluster):
    """
    Create a dedicated Core Helm installation.

    Use this if the test case requires a customized Helm installation or
    would interfere due to it behavior with other test cases and hence can't
    use the session (shared) Core Helm installation.

    Example: Your test requires 5 replicas or your test deletes pods.
    """
    # Only run if specifically requested via @pytest.mark.dedicated_helm_install
    if request.node.get_closest_marker("dedicated_helm_install"):
        helm_values = getattr(request, "param", {})
        data = deploy_app(kind_cluster, helm_values=helm_values)
        yield data
        cleanup_app(data, kind_cluster)
    else:
        yield None


@fixture(scope="function")
def kind_app_setup(request, session_helm_install, dedicated_helm_install):
    """
    Dynamically injects the required environment variables from whichever kind setup is active.
    """
    # Use dedicated if it exists (not None), otherwise fall back to session
    active_setup = (
        dedicated_helm_install
        if dedicated_helm_install is not None
        else session_helm_install
    )

    if active_setup:
        if active_setup == dedicated_helm_install:
            print(f"\n[DEBUG] Using DEDICATED install at {active_setup['url']}")
        else:
            print(f"\n[DEBUG] Using SESSION install at {active_setup['url']}")

    backend = getattr(request, "param", "docker-compose")
    if backend == "kind" and active_setup:
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
        old_env = {k: os.environ.get(k) for k in env_vars}
        os.environ.update(env_vars)

        yield "kind"

        # Restore env
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    else:
        yield "docker-compose"
