import inspect
import logging
from importlib import import_module
from pathlib import Path
from platform import python_version, release, system
from sys import modules
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel

from firebolt import __version__


class ConnectorVersions(BaseModel):
    """
    Verify correct parameter types
    """

    clients: List[Tuple[str, str]]
    drivers: List[Tuple[str, str]]


logger = logging.getLogger(__name__)


# Name, Function, Path, module
CLIENT_MAP = [
    (
        "Airflow",
        "get_conn",
        Path("firebolt_provider/hooks/firebolt.py"),
        "firebolt_provider",
    ),
    (
        "AirbyteDestination",
        "establish_connection",
        Path("destination_firebolt/destination.py"),
        "",
    ),
    (
        "AirbyteDestination",
        "establish_async_connection",
        Path("destination_firebolt/destination.py"),
        "",
    ),
    ("AirbyteSource", "establish_connection", Path("source_firebolt/source.py"), ""),
    (
        "AirbyteSource",
        "establish_async_connection",
        Path("source_firebolt/source.py"),
        "",
    ),
    ("FireboltCLI", "create_connection", Path("firebolt_cli/utils.py"), "firebolt_cli"),
    (
        "DBT",
        "open",
        Path("dbt/adapters/firebolt/connections.py"),
        "dbt.adapters.firebolt",
    ),
    (
        "Superset",
        "",  # connection is created in multiple places
        Path("superset/models/core.py"),
        "",
    ),
    (
        "Redash",
        "run_query",
        Path("redash/query_runner/firebolt.py"),
        "redash",
    ),
    (
        "Prefect",
        "run",
        Path("prefect/tasks/firebolt/firebolt.py"),
        "prefect",
    ),
]

DRIVER_MAP = [
    ("SQLAlchemy", "connect", Path("sqlalchemy/engine/default.py"), "firebolt_db"),
]


def _os_compare(file: Path, expected: Path) -> bool:
    """
    System-independent path comparison.

    Args:
        file: file path to check against
        expected: expected file path

    Returns:
        True if file ends with path
    """
    return file.parts[-len(expected.parts) :] == expected.parts


def get_sdk_properties() -> Tuple[str, str, str, str]:
    """
    Detect Python, OS and SDK versions.

    Returns:
        Python version, SDK version, OS name and "ciso" if imported
    """
    py_version = python_version()
    sdk_version = __version__
    os_version = f"{system()} {release()}"
    ciso = "ciso8601" if "ciso8601" in modules.keys() else ""
    logger.debug(
        "Python %s detected. SDK %s OS %s %s",
        py_version,
        sdk_version,
        os_version,
        ciso,
    )
    return (py_version, sdk_version, os_version, ciso)


def detect_connectors(
    connector_map: List[Tuple[str, str, Path, str]]
) -> Dict[str, str]:
    """
    Detect which connectors are running the code by parsing the stack.
    Exceptions are ignored since this is intended for logging only.
    """
    connectors: Dict[str, str] = {}
    stack = inspect.stack()
    for f in stack:
        try:
            for name, func, path, version_path in connector_map:
                if (not func or f.function == func) and _os_compare(
                    Path(f.filename), path
                ):
                    if version_path:
                        m = import_module(version_path)
                        connectors[name] = m.__version__  # type: ignore
                    else:
                        # Some connectors don't have versions specified
                        connectors[name] = ""
                    # No need to carry on if connector is detected
                    break
        except Exception:
            logger.debug(
                "Failed to extract version from %s in %s", f.function, f.filename
            )
    return connectors


def format_as_user_agent(drivers: Dict[str, str], clients: Dict[str, str]) -> str:
    """
    Return a representation of a stored tracking data as a user-agent header.

    Args:
        connectors: Dictionary of connector to version mappings

    Returns:
        String of the current detected connector stack.
    """
    py, sdk, os, ciso = get_sdk_properties()
    sdk_format = f"PythonSDK/{sdk} (Python {py}; {os}; {ciso})"
    driver_format = "".join(
        [f" {connector}/{version}" for connector, version in drivers.items()]
    )
    client_format = "".join(
        [f"{connector}/{version} " for connector, version in clients.items()]
    )
    return client_format + sdk_format + driver_format


def get_user_agent_header(
    user_drivers: Optional[List[Tuple[str, str]]] = [],
    user_clients: Optional[List[Tuple[str, str]]] = [],
) -> str:
    """
    Return a user agent header with connector stack and system information.

    Args:
        user_drivers(Optional): User-supplied list of tuples of all drivers
            and their versions intended for tracking. Driver is a programmatic
            module that facilitates interaction between a clients and underlying
            database.
        user_clients(Optional): User-supplied list of tuples of all clients
            and their versions intended for tracking. Client is a user-facing
            module or application that allows interaction with the database
            via drivers or directly.

    Returns:
        String representation of a user-agent tracking information
    """
    drivers = detect_connectors(DRIVER_MAP)
    clients = detect_connectors(CLIENT_MAP)
    logger.debug(
        "Detected running with drivers: %s and clients %s ", str(drivers), str(clients)
    )
    # Override auto-detected connectors with info provided manually
    versions = ConnectorVersions(clients=user_clients, drivers=user_drivers)
    for name, version in versions.clients:
        clients[name] = version
    for name, version in versions.drivers:
        drivers[name] = version
    return format_as_user_agent(drivers, clients)
