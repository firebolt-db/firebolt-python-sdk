import inspect
import logging
from platform import python_version, release, system
from sys import modules
from typing import Dict, Tuple

from firebolt import __version__
from firebolt.utils.util import cached_property

logger = logging.getLogger(__name__)


def _os_compare(file: str, expected: str) -> bool:
    """
    System-independent path comparison.

    Args:
        file: file path to check against
        expected: expected file path

    Returns:
        True if file ends with path
    """
    return file.endswith(expected) or file.endswith(expected.replace("/", "\\"))


def _is_cli(func: str, file: str) -> bool:
    return func == "create_connection" and _os_compare(file, "firebolt_cli/utils.py")


def _is_alchemy(func: str, file: str) -> bool:
    return func == "connect" and _os_compare(file, "sqlalchemy/engine/default.py")


def _is_airbyte_source(func: str, file: str) -> bool:
    return (
        func == "establish_connection" or func == "establish_async_connection"
    ) and _os_compare(file, "source_firebolt/source.py")


def _is_airbyte_destination(func: str, file: str) -> bool:
    return (
        func == "establish_connection" or func == "establish_async_connection"
    ) and _os_compare(file, "destination_firebolt/destination.py")


def _is_airflow(func: str, file: str) -> bool:
    return func == "get_conn" and _os_compare(
        file, "firebolt_provider/hooks/firebolt.py"
    )


def _is_dbt(func: str, file: str) -> bool:
    return func == "open" and _os_compare(file, "dbt/adapters/firebolt/connections.py")


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


class UsageTracker:
    """
    Tracking SDK usage by detecting the parent connector and system specs.
    """

    def __init__(self) -> None:
        self.connectors: Dict[str, str] = {}
        stack = inspect.stack()
        for f in stack:
            try:
                if _is_cli(f.function, f.filename):
                    from firebolt_cli import __version__  # type: ignore

                    self.connectors["FireboltCLI"] = __version__
                elif _is_alchemy(f.function, f.filename):
                    from firebolt_db import __version__  # type: ignore

                    self.connectors["SQLAlchemy"] = __version__
                elif _is_airbyte_source(f.function, f.filename):
                    # Airbyte version is stored in a Docker label,
                    # can't easily extract it
                    self.connectors["AibyteSource"] = ""
                elif _is_airbyte_destination(f.function, f.filename):
                    # Airbyte version is stored in a Docker label,
                    # can't easily extract it
                    self.connectors["AibyteDestination"] = ""
                elif _is_airflow(f.function, f.filename):
                    from firebolt_provider import __version__  # type: ignore

                    self.connectors["Airflow"] = __version__
                elif _is_dbt(f.function, f.filename):
                    from dbt.adapters.firebolt import (  # type: ignore
                        __version__,
                    )

                    self.connectors["DBT"] = __version__
            except Exception:
                logger.debug(
                    "Failed to extract version from %s in %s", f.function, f.filename
                )
        logger.debug("Detected running from packages: %s", str(self.connectors))

    def add_connector_information(self, connector: str, version: str) -> None:
        """
        Manually add/override a connector for tracking. Useful for tracing
        unofficial implementations or improving the auto-detected information.

        Args:
            connector: Connector name
            version: Relevant version e.g. "1.0.1-alpha"
        """
        self.connectors[connector] = version
        # Invalidate cache
        UsageTracker.user_agent.fget.cache_clear()
        logger.debug("Manually added: %s ver:%s", connector, version)

    @cached_property
    def user_agent(self) -> str:
        """
        Return a representation of a stored tracking data as a user-agent header.

        Returns:
            String of the current detected connector stack.
        """
        py, sdk, os, ciso = get_sdk_properties()
        sdk_format = f"PythonSDK/{sdk} (Python {py}; {os}; {ciso})"
        connector_format = " ".join(
            [f"{connector}/{version}" for connector, version in self.connectors.items()]
        )
        connector_format = " " + connector_format if connector_format else ""
        return sdk_format + connector_format
