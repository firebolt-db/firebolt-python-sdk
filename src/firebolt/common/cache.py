import os
from typing import Any, Callable, Dict, Generic, Optional, Protocol, TypeVar

from firebolt.utils.util import DatabaseInfo, EngineInfo

T = TypeVar("T")


class ReprCacheable(Protocol):
    def __repr__(self) -> str:
        ...


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
        return self._cache.get(s_key)

    @noop_if_disabled
    def set(self, key: ReprCacheable, value: T) -> None:
        if not self.disabled:
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


class CacheController:
    def __init__(self) -> None:
        self._engine_cache = UtilCache[EngineInfo](cache_name="engine_info")

        self._system_engine_cache = UtilCache[EngineInfo](cache_name="system_engine")

        self._database_cache = UtilCache[DatabaseInfo](cache_name="database_info")

    @property
    def engine_cache(self) -> UtilCache[EngineInfo]:
        """Get the engine cache."""
        return self._engine_cache

    @property
    def database_cache(self) -> UtilCache[DatabaseInfo]:
        """Get the database cache."""
        return self._database_cache

    @property
    def system_engine_cache(self) -> UtilCache[EngineInfo]:
        """Get the system engine cache."""
        return self._system_engine_cache

    def enable(self) -> None:
        """Enable the cache."""
        self._engine_cache.enable()
        self._system_engine_cache.enable()
        self._database_cache.enable()

    def disable(self) -> None:
        """Disable the cache."""
        self._engine_cache.disable()
        self._system_engine_cache.disable()
        self._database_cache.disable()

    def clear(self) -> None:
        """Clear all caches."""
        self._engine_cache.clear()
        self._system_engine_cache.clear()
        self._database_cache.clear()


_firebolt_cache = CacheController()
