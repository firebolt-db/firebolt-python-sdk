from typing import Optional
from urllib.parse import ParseResult, urlparse

from firebolt.utils.exception import ConfigurationError


def parse_firebolt_core_url(url: Optional[str] = None) -> ParseResult:
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
    if not url:
        # Default values
        return ParseResult(
            scheme="http",
            netloc="localhost:3473",
            path="",
            params="",
            query="",
            fragment="",
        )

    # Parse URL
    try:
        parsed = urlparse(url)
        # Protocol
        if parsed.scheme:
            if parsed.scheme not in ["http", "https"]:
                raise ConfigurationError(
                    f"Invalid protocol: {parsed.scheme}. Must be 'http' or 'https'."
                )

        if parsed.port and not (1 <= parsed.port <= 65535):
            raise ConfigurationError(
                f"Invalid port: {parsed.port}. Must be between 1 and 65535."
            )
    except Exception as e:
        if isinstance(e, ConfigurationError):
            raise
        raise ConfigurationError(
            f"Invalid URL format: {url}. Expected format: protocol://host:port"
        ) from e

    return parsed
