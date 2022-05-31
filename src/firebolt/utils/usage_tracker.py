import inspect
import logging
from functools import cached_property
from platform import python_version, release, system
from sys import modules
from typing import Dict, Tuple

from firebolt import __version__

logger = logging.getLogger(__name__)


def os_compare(file: str, path: str) -> bool:
    return file.endswith(path) or file.endswith(path.replace("/", "\\"))


def is_cli(func: str, file: str) -> bool:
    return func == "create_connection" and os_compare(file, "firebolt_cli/utils.py")


def is_alchemy(func: str, file: str) -> bool:
    return func == "connect" and os_compare(file, "sqlalchemy/engine/default.py")


def is_airbyte_source(func: str, file: str) -> bool:
    return (
        func == "establish_connection" or func == "establish_async_connection"
    ) and os_compare(file, "source_firebolt/source.py")


def is_airbyte_destination(func: str, file: str) -> bool:
    return (
        func == "establish_connection" or func == "establish_async_connection"
    ) and os_compare(file, "destination_firebolt/destination.py")


def is_airflow(func: str, file: str) -> bool:
    return func == "get_conn" and os_compare(
        file, "firebolt_provider/hooks/firebolt.py"
    )


def is_dbt(func: str, file: str) -> bool:
    return func == "open" and os_compare(file, "dbt/adapters/firebolt/connections.py")


def get_sdk_properties() -> Tuple[str, str, str, str]:
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
    def __init__(self) -> None:
        self.connectors: Dict[str, str] = {}
        stack = inspect.stack()
        for f in stack:
            try:
                if is_cli(f.function, f.filename):
                    from firebolt_cli import __version__  # type: ignore

                    self.connectors["FireboltCLI"] = __version__
                elif is_alchemy(f.function, f.filename):
                    from firebolt_db import __version__  # type: ignore

                    self.connectors["SQLAlchemy"] = __version__
                elif is_airbyte_source(f.function, f.filename):
                    self.connectors["AibyteSource"] = ""  # TODO: version?
                elif is_airbyte_destination(f.function, f.filename):
                    self.connectors["AibyteDestination"] = ""
                elif is_airflow(f.function, f.filename):
                    from firebolt_provider import __version__  # type: ignore

                    self.connectors["Airflow"] = __version__
                elif is_dbt(f.function, f.filename):
                    from dbt.adapters.firebolt import (  # type: ignore
                        __version__,
                    )

                    self.connectors["DBT"] = __version__
            except Exception:
                logger.debug(
                    "Failed to extract version from %s in %s", f.function, f.filename
                )
        logger.debug("Detected running from packages: %s", str(self.connectors))

    def add_connector_information(self, caller: str, version: str) -> None:
        self.connectors[caller] = version
        # Invalidate cache
        if getattr(self, "user_agent"):
            del self.user_agent
        logger.debug("Manually added: %s ver:%s", caller, version)

    @cached_property
    def user_agent(self) -> str:
        py, sdk, os, ciso = get_sdk_properties()
        sdk_format = f"PythonSDK/{sdk} (Python {py}; {os}; {ciso})"
        connector_format = " ".join(
            [f"{connector}/{version}" for connector, version in self.connectors.items()]
        )
        connector_format = " " + connector_format if connector_format else ""
        return sdk_format + connector_format
