"""Helper functions for cache-related tests."""
from typing import Optional

from firebolt.utils.cache import (
    ConnectionInfo,
    SecureCacheKey,
    _firebolt_cache,
)


def get_cached_token(
    principal: str, secret: str, account_name: Optional[str] = None
) -> Optional[str]:
    """Get cached token for the given credentials.

    This is a test helper function for backward compatibility.

    Args:
        principal: Username or client ID
        secret: Password or client secret
        account_name: Account name (optional)

    Returns:
        Cached token if available, None otherwise
    """
    cache_key = SecureCacheKey([principal, secret, account_name], secret)
    connection_info = _firebolt_cache.get(cache_key)

    if connection_info and connection_info.token:
        return connection_info.token
    return None


def cache_token(
    principal: str,
    secret: str,
    token: str,
    expiry: Optional[int] = None,
    account_name: Optional[str] = None,
) -> None:
    """Cache token for the given credentials.

    This is a test helper function for backward compatibility.

    Args:
        principal: Username or client ID
        secret: Password or client secret
        token: Token to cache
        expiry: Token expiry time (ignored, we use our own expiry)
        account_name: Account name (optional)
    """
    cache_key = SecureCacheKey([principal, secret, account_name], secret)

    # Get existing connection info or create new one
    connection_info = _firebolt_cache.get(cache_key)
    if connection_info is None:
        connection_info = ConnectionInfo(id="NONE")

    # Update token information
    connection_info.token = token

    # Cache it
    _firebolt_cache.set(cache_key, connection_info)
