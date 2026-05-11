import getpass
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from json import JSONDecodeError
from json import dumps as json_dumps
from json import loads as json_loads
from os import makedirs, path
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

from firebolt.utils.file_operations import (
    FernetEncrypter,
    generate_encrypted_file_name,
    generate_salt,
)

T = TypeVar("T")

# Cache expiry configuration
CACHE_EXPIRY_SECONDS = 3600  # 1 hour
APPNAME = "fireboltDriver"

logger = logging.getLogger(__name__)


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
    token: Optional[str] = None

    def __post_init__(self) -> None:
        """
        Post-initialization processing to convert dicts to dataclasses.
        """
        if self.system_engine and isinstance(self.system_engine, dict):
            self.system_engine = EngineInfo(**self.system_engine)

        # Convert dict values to dataclasses, keep existing dataclass objects
        new_databases = {}
        for k, db in self.databases.items():
            if isinstance(db, dict):
                new_databases[k] = DatabaseInfo(**db)
            else:
                new_databases[k] = db
        self.databases = new_databases

        # Convert dict values to dataclasses, keep existing dataclass objects
        new_engines = {}
        for k, engine in self.engines.items():
            if isinstance(engine, dict):
                new_engines[k] = EngineInfo(**engine)
            else:
                new_engines[k] = engine
        self.engines = new_engines


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
    def set(self, key: ReprCacheable, value: T, preserve_expiry: bool = False) -> None:
        if not self.disabled:
            # Set expiry_time for ConnectionInfo objects
            if hasattr(value, "expiry_time") and (
                not preserve_expiry or value.expiry_time is None
            ):
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


def get_cache_data_dir(appname: str = APPNAME) -> str:
    """
    Return the directory for storing cache files based on the OS.
    Mac: use $TMPDIR, fallback to /tmp/<appname>
    Windows: use the environment variable TEMP or if not defined C:\\Temp
    Linux: use $XDG_RUNTIME_DIR, fallback to /tmp/<user_home>
    """

    if sys.platform == "darwin":
        tmpdir = os.environ.get("TMPDIR")
        if tmpdir:
            return os.path.join(tmpdir, appname)
        # fallback
        return os.path.join("/tmp", appname)
    elif sys.platform.startswith("win"):
        # Python doesn't expose java.io.tmpdir, but os.environ['TEMP'] is standard
        tmpdir = os.environ.get("TEMP")
        if tmpdir:
            return os.path.join(tmpdir, appname)
        # fallback
        return os.path.join("C:\\Temp", appname)
    else:
        # Assume Linux/Unix
        xdg_dir = os.environ.get("XDG_RUNTIME_DIR")
        if xdg_dir:
            return os.path.join(xdg_dir, appname)
    # fallback: /tmp/<username>
    username = getpass.getuser()
    return os.path.join("/tmp", username, appname)


class FileBasedCache:
    """
    File-based cache that persists to disk with encryption.
    Uses composition to combine in-memory caching with persistent storage
    using encrypted files.
    """

    def __init__(self, memory_cache: UtilCache[ConnectionInfo], cache_name: str = ""):
        self.memory_cache = memory_cache
        self._data_dir = get_cache_data_dir(APPNAME)
        makedirs(self._data_dir, exist_ok=True)
        # FileBasedCache has its own disabled state, independent of memory cache
        cache_env_var = f"FIREBOLT_SDK_DISABLE_CACHE_${cache_name}"
        self.disabled = os.getenv("FIREBOLT_SDK_DISABLE_CACHE", False) or os.getenv(
            cache_env_var, False
        )

    def disable(self) -> None:
        """Disable the file-based cache."""
        self.disabled = True

    def enable(self) -> None:
        """Enable the file-based cache."""
        self.disabled = False

    def _get_file_path(self, key: SecureCacheKey) -> str:
        """Get the file path for a cache key."""
        cache_key = self.memory_cache.create_key(key)
        encrypted_filename = generate_encrypted_file_name(cache_key, key.encryption_key)
        return path.join(self._data_dir, encrypted_filename)

    def _read_data_json(self, file_path: str, encrypter: FernetEncrypter) -> dict:
        """Read and decrypt JSON data from file."""
        if not path.exists(file_path):
            return {}

        try:
            with open(file_path, "r") as f:
                encrypted_data = f.read()

            decrypted_data = encrypter.decrypt(encrypted_data)
            if decrypted_data is None:
                logger.debug("Decryption failed for %s", file_path)
                return {}

            return json_loads(decrypted_data) if decrypted_data else {}
        except (JSONDecodeError, IOError) as e:
            logger.debug(
                "Failed to read or decode data from %s error: %s", file_path, e
            )
            return {}

    def _write_data_json(
        self, file_path: str, data: dict, encrypter: FernetEncrypter
    ) -> None:
        """Encrypt and write JSON data to file."""
        try:
            json_str = json_dumps(data)
            logger.debug("Writing data to %s", file_path)
            encrypted_data = encrypter.encrypt(json_str)
            with open(file_path, "w") as f:
                f.write(encrypted_data)
        except (IOError, OSError) as e:
            # Silently proceed if we can't write to disk
            logger.debug("Failed to write data to %s error: %s", file_path, e)

    def get(self, key: SecureCacheKey) -> Optional[ConnectionInfo]:
        """Get value from cache, checking both memory and disk."""
        if self.disabled:
            return None

        # First try memory cache
        memory_result = self.memory_cache.get(key)
        if memory_result is not None:
            logger.debug("Cache hit in memory")
            return memory_result

        # If not in memory, try to load from disk
        file_path = self._get_file_path(key)
        encrypter = FernetEncrypter(generate_salt(), key.encryption_key)
        raw_data = self._read_data_json(file_path, encrypter)
        if not raw_data:
            return None

        logger.debug("Cache hit on disk")
        data = ConnectionInfo(**raw_data)

        # Check if the loaded data is expired
        if self.memory_cache._is_expired(data):
            # Data is expired, delete the file and return None
            try:
                if path.exists(file_path):
                    os.remove(file_path)
                    logger.debug("Deleted expired file %s", file_path)
            except OSError:
                logger.debug("Failed to delete expired file %s", file_path)
            return None

        # Data is not expired, add to memory cache preserving original expiry time
        self.memory_cache.set(key, data, preserve_expiry=True)
        return data

    def set(self, key: SecureCacheKey, value: ConnectionInfo) -> None:
        """Set value in both memory and disk cache."""
        if self.disabled:
            return

        logger.debug("Setting value in cache")
        # First set in memory
        self.memory_cache.set(key, value)

        file_path = self._get_file_path(key)
        encrypter = FernetEncrypter(generate_salt(), key.encryption_key)
        data = asdict(value)

        self._write_data_json(file_path, data, encrypter)

    def delete(self, key: SecureCacheKey) -> None:
        """Delete value from both memory and disk cache."""
        if self.disabled:
            return

        # Delete from memory
        self.memory_cache.delete(key)

        # Delete from disk
        file_path = self._get_file_path(key)
        try:
            if path.exists(file_path):
                os.remove(file_path)
        except OSError:
            logger.debug("Failed to delete file %s", file_path)
            # Silently proceed if we can't delete the file

    def clear(self) -> None:
        # Clear memory only, as deleting every file is not safe
        logger.debug("Clearing memory cache")
        self.memory_cache.clear()


_firebolt_cache = FileBasedCache(
    UtilCache[ConnectionInfo](cache_name="memory_cache"), cache_name="file_cache"
)
