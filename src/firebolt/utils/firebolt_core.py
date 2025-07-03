from typing import Dict, Optional
from urllib.parse import urlparse

from firebolt.utils.exception import ConfigurationError


def parse_firebolt_core_url(url: Optional[str] = None) -> Dict[str, str]:
    """Parse a Firebolt Core URL into its components.

    Args:
        url (str, optional): URL in format protocol://host:port
            Protocol defaults to http, host defaults to localhost, port
            defaults to 3473.

    Returns:
        Dict[str, str]: Dictionary with protocol, host, and port keys

    Raises:
        ConfigurationError: If the URL is invalid
    """
    # Default values
    result = {"protocol": "http", "host": "localhost", "port": "3473"}

    if not url:
        return result

    # Parse URL
    try:
        parsed = urlparse(url)

        # Protocol
        if parsed.scheme:
            if parsed.scheme not in ["http", "https"]:
                raise ConfigurationError(
                    f"Invalid protocol: {parsed.scheme}. Must be 'http' or 'https'."
                )
            result["protocol"] = parsed.scheme

        # Handle host and port - using urlparse to properly handle IPv6 addresses
        if parsed.netloc:
            # For IPv6, netloc will be like [::1]:3473
            # For other hosts, netloc will be like localhost:3473
            host = parsed.hostname  # This properly extracts hostname (even IPv6)
            port = parsed.port  # This properly extracts port number

            if host:
                # Preserve brackets for IPv6 addresses in the host field
                if ":" in host and not host.startswith("["):
                    result["host"] = f"[{host}]"
                else:
                    result["host"] = host

            if port is not None:
                if not (1 <= port <= 65535):
                    raise ConfigurationError(
                        f"Invalid port: {port}. Must be between 1 and 65535."
                    )
                result["port"] = str(port)

    except Exception as e:
        if isinstance(e, ConfigurationError):
            raise
        raise ConfigurationError(
            f"Invalid URL format: {url}. Expected format: protocol://host:port"
        ) from e

    return result


def get_firebolt_core_connection_parameters(
    url: Optional[str] = None, database: Optional[str] = None
) -> Dict[str, str]:
    """Get connection parameters for Firebolt Core.

    Args:
        url (str, optional): URL in format protocol://host:port
        database (str, optional): Database name, defaults to "firebolt"

    Returns:
        Dict[str, str]: Dictionary with connection parameters

    Raises:
        ConfigurationError: If required parameters are missing or invalid
    """
    # Parse URL
    parsed_url = parse_firebolt_core_url(url)

    # Firebolt Core has a default database
    if not database:
        database = "firebolt"

    # Create connection parameters
    connection_params = {
        "protocol": parsed_url["protocol"],
        "host": parsed_url["host"],
        "port": parsed_url["port"],
        "database": database,
        "connection_type": "firebolt-core",
    }

    return connection_params


def get_firebolt_core_engine_url(url: Optional[str] = None) -> str:
    """Get the engine URL for Firebolt Core.

    Args:
        url (str, optional): URL in format protocol://host:port

    Returns:
        str: The full engine URL
    """
    parsed_url = parse_firebolt_core_url(url)

    # Ensure IPv6 addresses are properly enclosed in square brackets in the final URL
    host = parsed_url["host"]
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        host = f"[{host}]"
    else:
        host = parsed_url["host"]

    return f"{parsed_url['protocol']}://{host}:{parsed_url['port']}"
