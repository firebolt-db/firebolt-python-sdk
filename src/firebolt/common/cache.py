import os
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
)

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


_firebolt_system_engine_cache = UtilCache[Tuple[str, Dict[str, str]]](
    cache_name="system_engine"
)
