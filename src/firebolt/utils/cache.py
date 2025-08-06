import os
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    TypeVar,
)

T = TypeVar("T")

# Cache expiry configuration
CACHE_EXPIRY_SECONDS = 3600  # 1 hour


class ReprCacheable(Protocol):
    def __repr__(self) -> str:
        ...


@dataclass
class EngineInfo:
    """Class to hold engine information for caching."""

    url: str
    params: Dict[str, str]


@dataclass
class DatabaseInfo:
    """Class to hold database information for caching."""

    name: str


@dataclass
class ConnectionInfo:
    """Class to hold connection information for caching."""

    id: str
    expiry_time: Optional[int] = None
    system_engine: Optional[EngineInfo] = None
    databases: Dict[str, DatabaseInfo] = field(default_factory=dict)
    engines: Dict[str, EngineInfo] = field(default_factory=dict)


def noop_if_disabled(func: Callable) -> Callable:
    """Decorator to make function do nothing if the cache is disabled."""

    def wrapper(self: "UtilCache", *args: Any, **kwargs: Any) -> Any:
        if not self.disabled:
            return func(self, *args, **kwargs)

    return wrapper


class UtilCache(Generic[T]):
    """
    Generic cache implementation to store key-value pairs.
    Created to abstract the cache implementation in case we find a better
    solution in the future.
    """

    def __init__(self, cache_name: str = "") -> None:
        self._cache: Dict[str, T] = {}
        # Allow disabling cache if we have no direct access to the constructor
        self.disabled = os.getenv("FIREBOLT_SDK_DISABLE_CACHE", False) or os.getenv(
            f"FIREBOLT_SDK_DISABLE_CACHE_${cache_name}", False
        )

    def disable(self) -> None:
        self.disabled = True

    def enable(self) -> None:
        self.disabled = False

    def get(self, key: ReprCacheable) -> Optional[T]:
        if self.disabled:
            return None
        s_key = self.create_key(key)
        value = self._cache.get(s_key)

        if value is not None and self._is_expired(value):
            # Cache miss due to expiry - delete the expired item
            del self._cache[s_key]
            return None

        return value

    def _is_expired(self, value: T) -> bool:
        """Check if a cached value has expired."""
        # Only check expiry for ConnectionInfo objects that have expiry_time
        if hasattr(value, "expiry_time") and value.expiry_time is not None:
            current_time = int(time.time())
            return current_time >= value.expiry_time
        return False

    @noop_if_disabled
    def set(self, key: ReprCacheable, value: T) -> None:
        if not self.disabled:
            # Set expiry_time for ConnectionInfo objects
            if hasattr(value, "expiry_time"):
                current_time = int(time.time())
                value.expiry_time = current_time + CACHE_EXPIRY_SECONDS

            s_key = self.create_key(key)
            self._cache[s_key] = value

    @noop_if_disabled
    def delete(self, key: ReprCacheable) -> None:
        s_key = self.create_key(key)
        if s_key in self._cache:
            del self._cache[s_key]

    @noop_if_disabled
    def clear(self) -> None:
        self._cache.clear()

    def create_key(self, obj: ReprCacheable) -> str:
        return repr(obj)

    def __contains__(self, key: str) -> bool:
        """Support for 'in' operator to check if key is present in cache."""
        if self.disabled:
            return False
        return key in self._cache


class SecureCacheKey(ReprCacheable):
    """A secure cache key that can be used for caching sensitive information."""

    def __init__(self, key_elements: List[Optional[str]], encryption_key: str):
        self.key = "#".join(str(e) for e in key_elements)
        self.encryption_key = encryption_key

    def __repr__(self) -> str:
        return f"SecureCacheKey({self.key})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecureCacheKey):
            return self.key == other.key
        return False

    def __hash__(self) -> int:
        return hash(self.key)


_firebolt_cache = UtilCache[ConnectionInfo](cache_name="connection_info")
