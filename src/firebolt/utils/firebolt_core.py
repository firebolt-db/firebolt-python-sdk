import os
import sys
from ssl import (
    PROTOCOL_TLS_CLIENT,
    Purpose,
    SSLContext,
    TLSVersion,
    create_default_context,
)
from typing import Optional, Union
from urllib.parse import ParseResult, urlparse

from firebolt.utils.exception import ConfigurationError


def get_core_certificate_context() -> Union[SSLContext, bool]:
    """Get the SSL context for Firebolt Core connections."""
    ctx: Union[SSLContext, bool] = True  # Default context for SSL verification
    if os.getenv("SSL_CERT_FILE"):
        ctx = create_default_context(
            Purpose.SERVER_AUTH, cafile=os.getenv("SSL_CERT_FILE")
        )
        ctx.minimum_version = TLSVersion.TLSv1_2
    elif sys.version_info >= (3, 10):
        # Can import truststore only if python is 3.10 or higher
        import truststore

        ctx = truststore.SSLContext(PROTOCOL_TLS_CLIENT)
    else:
        raise ConfigurationError(
            "Not able to use system certificate store for Firebolt Core."
            " on Python < 3.10, you may need to set"
            " SSL_CERT_FILE environment variable pointing to your .pem file."
        )
    return ctx


def validate_firebolt_core_parameters(
    account_name: Optional[str] = None,
    engine_name: Optional[str] = None,
    engine_url: Optional[str] = None,
) -> None:
    """Validate that no incompatible parameters are provided with
    FireboltCore authentication.

    Args:
        account_name (Optional[str]): Account name parameter to validate
        engine_name (Optional[str]): Engine name parameter to validate
        engine_url (Optional[str]): Engine URL parameter to validate

    Raises:
        ConfigurationError: If any incompatible parameters are provided
    """
    forbidden_params = {}
    if account_name:
        forbidden_params["account_name"] = account_name
    if engine_name:
        forbidden_params["engine_name"] = engine_name
    if engine_url:
        forbidden_params["engine_url"] = engine_url

    if forbidden_params:
        param_list = ", ".join(f"'{p}'" for p in forbidden_params.keys())
        raise ConfigurationError(
            f"Parameters {param_list} are not compatible with Firebolt Core "
            "authentication. These parameters should not be provided when "
            "using Firebolt Core."
        )


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
