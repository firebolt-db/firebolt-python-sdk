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

    versions: List[Tuple[str, str]]


logger = logging.getLogger(__name__)


CONNECTOR_MAP = [
    (
        "DBT",
        "open",
        Path("dbt/adapters/firebolt/connections.py"),
        "dbt.adapters.firebolt",
    ),
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
    ("SQLAlchemy", "connect", Path("sqlalchemy/engine/default.py"), "firebolt_db"),
    ("FireboltCLI", "create_connection", Path("firebolt_cli/utils.py"), "firebolt_cli"),
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


def detect_connectors() -> Dict[str, str]:
    """
    Detect which connectors are running the code by parsing the stack.
    Exceptions are ignored since this is intended for logging only.
    """
    connectors: Dict[str, str] = {}
    stack = inspect.stack()
    for f in stack:
        try:
            for name, func, path, version_path in CONNECTOR_MAP:
                if f.function == func and _os_compare(Path(f.filename), path):
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


def format_as_user_agent(connectors: Dict[str, str]) -> str:
    """
    Return a representation of a stored tracking data as a user-agent header.

    Args:
        connectors: Dictionary of connector to version mappings

    Returns:
        String of the current detected connector stack.
    """
    py, sdk, os, ciso = get_sdk_properties()
    sdk_format = f"PythonSDK/{sdk} (Python {py}; {os}; {ciso})"
    connector_format = "".join(
        [f" {connector}/{version}" for connector, version in connectors.items()]
    )
    return sdk_format + connector_format


def get_user_agent_header(
    connector_versions: Optional[List[Tuple[str, str]]] = []
) -> str:
    """
    Return a user agent header with connector stack and system information.

    Args:
        connector_versions(Optional): User-supplied list of tuples of all connectors
            and their versions intended for tracking.

    Returns:
        String representation of a user-agent tracking information
    """
    connectors = detect_connectors()
    logger.debug("Detected running from packages: %s", str(connectors))
    # Override auto-detected connectors with info provided manually
    for name, version in ConnectorVersions(versions=connector_versions).versions:
        connectors[name] = version
    return format_as_user_agent(connectors)
