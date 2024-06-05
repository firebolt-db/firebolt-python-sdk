from typing import Dict, Generic, Optional, Protocol, Tuple, TypeVar

T = TypeVar("T")


class ReprCacheable(Protocol):
    def __repr__(self) -> str:
        ...


class UtilCache(Generic[T]):
    """
    Generic cache implementation to store key-value pairs.
    Created to abstract the cache implementation in case we find a better
    implementation in the future.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, T] = {}

    def get(self, key: ReprCacheable) -> Optional[T]:
        s_key = self.create_key(key)
        return self._cache.get(s_key)

    def set(self, key: ReprCacheable, value: T) -> None:
        s_key = self.create_key(key)
        self._cache[s_key] = value

    def delete(self, key: ReprCacheable) -> None:
        s_key = self.create_key(key)
        if s_key in self._cache:
            del self._cache[s_key]

    def clear(self) -> None:
        self._cache.clear()

    def create_key(self, obj: ReprCacheable) -> str:
        return repr(obj)

    def __contains__(self, key: str) -> bool:
        """Support for 'in' operator to check if key is present in cache."""
        return key in self._cache


_firebolt_system_engine_cache = UtilCache[Tuple[str, Dict[str, str]]]()
