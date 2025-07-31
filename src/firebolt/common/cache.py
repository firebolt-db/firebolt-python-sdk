import os
from dataclasses import dataclass, field
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
    id: Optional[str] = None
    expiry_time: Optional[int] = None
    system_engine_url: Optional[str] = None
    databases: Dict[str, DatabaseInfo] = field(default_factory=dict)
    engines: Dict[str, EngineInfo] = field(default_factory=dict)


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


class ConnectionInfoCache:
    """
    A wrapper around UtilCache to provide granular access to ConnectionInfo.
    """

    def __init__(self, cache_name: str = "") -> None:
        self._cache = UtilCache[ConnectionInfo](cache_name)

    def get(self, key: ReprCacheable) -> Optional[ConnectionInfo]:
        return self._cache.get(key)

    def set(self, key: ReprCacheable, value: ConnectionInfo) -> None:
        self._cache.set(key, value)

    def delete(self, key: ReprCacheable) -> None:
        self._cache.delete(key)

    def clear(self) -> None:
        self._cache.clear()

    def disable(self) -> None:
        self._cache.disable()
    
    def enable(self) -> None:
        self._cache.enable()

    def set_id(self, key: ReprCacheable, id: str) -> None:
        conn_info = self.get(key) or ConnectionInfo(id=id)
        conn_info.id = id
        self.set(key, conn_info)

    def get_id(self, key: ReprCacheable) -> Optional[str]:
        conn_info = self.get(key)
        return conn_info.id if conn_info else None

    def get_system_engine_url(self, key: ReprCacheable) -> Optional[str]:
        conn_info = self.get(key)
        return conn_info.system_engine_url if conn_info else None

    def set_system_engine_url(self, key: ReprCacheable, url: str) -> None:
        conn_info = self.get(key) or ConnectionInfo()
        conn_info.system_engine_url = url
        self.set(key, conn_info)

    def get_expiry_time(self, key: ReprCacheable) -> Optional[int]:
        conn_info = self.get(key)
        return conn_info.expiry_time if conn_info else None

    def get_engines(self, key: ReprCacheable) -> Optional[Dict[str, EngineInfo]]:
        conn_info = self.get(key)
        return conn_info.engines if conn_info else None

    def get_engine_by_name(
        self, key: ReprCacheable, engine_name: str
    ) -> Optional[EngineInfo]:
        engines = self.get_engines(key)
        return engines.get(engine_name) if engines else None

    def add_engine(self, key: ReprCacheable, engine_name: str, engine: EngineInfo) -> None:
        conn_info = self.get(key) or ConnectionInfo()
        conn_info.engines[engine_name] = engine
        self.set(key, conn_info)

    def get_databases(self, key: ReprCacheable) -> Optional[Dict[str, DatabaseInfo]]:
        conn_info = self.get(key)
        return conn_info.databases if conn_info else None

    def get_database_by_name(
        self, key: ReprCacheable, db_name: str
    ) -> Optional[DatabaseInfo]:
        databases = self.get_databases(key)
        return databases.get(db_name) if databases else None

    def add_database(
        self, key: ReprCacheable, db_name: str, database: DatabaseInfo
    ) -> None:
        conn_info = self.get(key) or ConnectionInfo()
        conn_info.databases[db_name] = database
        self.set(key, conn_info)


# _firebolt_system_engine_cache = UtilCache[Tuple[str, Dict[str, str]]](
#     cache_name="system_engine"
# )

_firebolt_cache = ConnectionInfoCache(cache_name="connection_info")