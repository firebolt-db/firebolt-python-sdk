import time
from typing import Generator
from unittest.mock import patch

from pytest import fixture, mark

from firebolt.utils.cache import (
    CACHE_EXPIRY_SECONDS,
    ConnectionInfo,
    SecureCacheKey,
    UtilCache,
)


@fixture
def cache() -> Generator[UtilCache[ConnectionInfo], None, None]:
    """Create a fresh cache instance for testing."""
    cache = UtilCache[ConnectionInfo](cache_name="test_cache")
    cache.enable()  # Ensure cache is enabled for tests
    yield cache
    cache.clear()


@fixture
def string_cache() -> Generator[UtilCache[str], None, None]:
    """Create a fresh string cache instance for testing."""
    cache = UtilCache[str](cache_name="string_test_cache")
    cache.enable()  # Ensure cache is enabled for tests
    yield cache
    cache.clear()


@fixture
def disabled_cache() -> UtilCache[ConnectionInfo]:
    """Create a disabled cache instance for testing."""
    cache = UtilCache[ConnectionInfo](cache_name="test_disabled_cache")
    cache.disable()
    return cache


@fixture
def sample_connection_info() -> ConnectionInfo:
    """Create a sample ConnectionInfo for testing."""
    return ConnectionInfo(id="test_connection")


@fixture
def sample_connection_info_with_expiry() -> ConnectionInfo:
    """Create a sample ConnectionInfo with explicit None expiry_time."""
    return ConnectionInfo(id="test_connection_with_expiry", expiry_time=None)


@fixture
def sample_cache_key() -> SecureCacheKey:
    """Create a sample cache key for testing."""
    return SecureCacheKey(["test", "key"], "secret")


@fixture
def additional_cache_keys():
    """Create additional cache keys for multi-key tests."""
    return {
        "key1": SecureCacheKey(["key1"], "secret"),
        "key2": SecureCacheKey(["key2"], "secret"),
        "key3": SecureCacheKey(["user", "other"], "secret"),
    }


@fixture
def fixed_time():
    """Provide a fixed timestamp for consistent testing."""
    return 1000000


@fixture
def test_string():
    """Provide a test string for non-ConnectionInfo cache tests."""
    return "test_value"


def test_cache_set_and_get(cache, sample_cache_key, sample_connection_info):
    """Test basic cache set and get operations."""
    # Test cache miss initially
    assert cache.get(sample_cache_key) is None

    # Set value and verify it's cached
    cache.set(sample_cache_key, sample_connection_info)
    cached_value = cache.get(sample_cache_key)

    assert cached_value is not None
    assert cached_value.id == "test_connection"
    assert cached_value.expiry_time is not None

    # Verify expiry_time is set to current time + CACHE_EXPIRY_SECONDS
    current_time = int(time.time())
    expected_expiry = current_time + CACHE_EXPIRY_SECONDS
    # Allow for small time difference due to test execution time
    assert abs(cached_value.expiry_time - expected_expiry) <= 2


def test_cache_expiry(cache, sample_cache_key, sample_connection_info, fixed_time):
    """Test that cache entries expire after the specified time."""
    with patch("time.time", return_value=fixed_time):
        # Set a value in the cache
        cache.set(sample_cache_key, sample_connection_info)
        cached_value = cache.get(sample_cache_key)

        # Verify it's cached and expiry_time is set
        assert cached_value is not None
        assert cached_value.expiry_time == fixed_time + CACHE_EXPIRY_SECONDS

    # Simulate time passing but not enough to expire (59 minutes)
    with patch("time.time", return_value=fixed_time + CACHE_EXPIRY_SECONDS - 60):
        cached_value = cache.get(sample_cache_key)
        assert cached_value is not None  # Should still be cached

    # Simulate time passing to exactly the expiry time
    with patch("time.time", return_value=fixed_time + CACHE_EXPIRY_SECONDS):
        cached_value = cache.get(sample_cache_key)
        assert cached_value is None  # Should be expired and removed

    # Verify the item is actually deleted from cache
    assert cache.create_key(sample_cache_key) not in cache._cache


def test_cache_expiry_past_expiry_time(
    cache, sample_cache_key, sample_connection_info, fixed_time
):
    """Test that cache entries are removed when accessed after expiry time."""
    with patch("time.time", return_value=fixed_time):
        cache.set(sample_cache_key, sample_connection_info)

    # Simulate time passing beyond expiry (2 hours)
    with patch("time.time", return_value=fixed_time + CACHE_EXPIRY_SECONDS + 3600):
        cached_value = cache.get(sample_cache_key)
        assert cached_value is None  # Should be expired

    # Verify the item is removed from the internal cache
    cache_key_str = cache.create_key(sample_cache_key)
    assert cache_key_str not in cache._cache


def test_cache_disabled_behavior(
    disabled_cache, sample_cache_key, sample_connection_info
):
    """Test that disabled cache doesn't store or retrieve values."""
    # Try to set a value
    disabled_cache.set(sample_cache_key, sample_connection_info)

    # Should return None even though we set a value
    assert disabled_cache.get(sample_cache_key) is None

    # Internal cache should be empty
    assert len(disabled_cache._cache) == 0


def test_cache_clear(
    cache, sample_cache_key, sample_connection_info, additional_cache_keys
):
    """Test that cache.clear() removes all entries."""
    # Add some entries
    cache.set(additional_cache_keys["key1"], sample_connection_info)
    cache.set(additional_cache_keys["key2"], sample_connection_info)

    # Verify entries exist
    assert cache.get(additional_cache_keys["key1"]) is not None
    assert cache.get(additional_cache_keys["key2"]) is not None

    # Clear cache
    cache.clear()

    # Verify all entries are gone
    assert cache.get(additional_cache_keys["key1"]) is None
    assert cache.get(additional_cache_keys["key2"]) is None
    assert len(cache._cache) == 0


def test_cache_delete(cache, sample_cache_key, sample_connection_info):
    """Test manual deletion of cache entries."""
    cache.set(sample_cache_key, sample_connection_info)
    assert cache.get(sample_cache_key) is not None

    cache.delete(sample_cache_key)
    assert cache.get(sample_cache_key) is None


def test_cache_contains_operator(cache, sample_cache_key, sample_connection_info):
    """Test the 'in' operator for cache."""
    cache_key_str = cache.create_key(sample_cache_key)

    # Initially not in cache
    assert cache_key_str not in cache

    # Add to cache
    cache.set(sample_cache_key, sample_connection_info)
    assert cache_key_str in cache

    # Test with disabled cache
    cache.disable()
    assert cache_key_str not in cache  # Should return False when disabled


def test_non_connection_info_objects(string_cache, sample_cache_key, test_string):
    """Test that non-ConnectionInfo objects don't get expiry_time set."""
    string_cache.set(sample_cache_key, test_string)

    # Should retrieve the string without expiry logic
    cached_value = string_cache.get(sample_cache_key)
    assert cached_value == test_string


def test_expiry_time_none_handling(
    cache, sample_cache_key, sample_connection_info_with_expiry, fixed_time
):
    """Test handling of ConnectionInfo with expiry_time set to None."""
    with patch("time.time", return_value=fixed_time):
        cache.set(sample_cache_key, sample_connection_info_with_expiry)

        # Should set expiry_time during set operation
        cached_value = cache.get(sample_cache_key)
        assert cached_value is not None
        assert cached_value.expiry_time is not None


def test_secure_cache_key_creation():
    """Test SecureCacheKey creation and repr."""
    key = SecureCacheKey(["user", "db", "engine"], "secret_key")
    assert key.key == "user#db#engine"
    assert key.encryption_key == "secret_key"
    assert repr(key) == "SecureCacheKey(user#db#engine)"


def test_secure_cache_key_equality():
    """Test SecureCacheKey equality comparison."""
    key1 = SecureCacheKey(["user", "db"], "secret1")
    key2 = SecureCacheKey(["user", "db"], "secret2")
    key3 = SecureCacheKey(["user", "other"], "secret1")

    assert key1 == key2  # Same key content, different encryption key
    assert key1 != key3  # Different key content
    assert key1 != "not_a_key"  # Different type


def test_secure_cache_key_hash():
    """Test SecureCacheKey hash functionality."""
    key1 = SecureCacheKey(["user", "db"], "secret1")
    key2 = SecureCacheKey(["user", "db"], "secret2")

    # Same key content should have same hash
    assert hash(key1) == hash(key2)

    # Should be usable in sets and dicts
    key_set = {key1, key2}
    assert len(key_set) == 1  # Should be treated as same key


def test_secure_cache_key_with_none_elements():
    """Test SecureCacheKey handling of None elements."""
    key = SecureCacheKey(["user", None, "engine"], "secret")
    assert key.key == "user#None#engine"


@mark.parametrize("cache_expiry_time_offset", [-60, 0, 3600])
def test_cache_expiry_parametrized(
    cache,
    sample_cache_key,
    sample_connection_info,
    fixed_time,
    cache_expiry_time_offset,
):
    """Test cache expiry behavior with different time offsets."""
    with patch("time.time", return_value=fixed_time):
        cache.set(sample_cache_key, sample_connection_info)

    # Test at different time offsets relative to expiry time
    check_time = fixed_time + CACHE_EXPIRY_SECONDS + cache_expiry_time_offset

    with patch("time.time", return_value=check_time):
        cached_value = cache.get(sample_cache_key)

        if cache_expiry_time_offset < 0:
            # Before expiry - should be cached
            assert cached_value is not None
        else:
            # At or after expiry - should be None
            assert cached_value is None


def test_cache_expiry_multiple_entries(cache, additional_cache_keys, fixed_time):
    """Test that expiry works correctly with multiple cache entries."""
    # Create separate ConnectionInfo objects to avoid shared state
    connection_info_1 = ConnectionInfo(id="test_connection_1")
    connection_info_2 = ConnectionInfo(id="test_connection_2")

    # Set multiple entries at different times
    with patch("time.time", return_value=fixed_time):
        cache.set(additional_cache_keys["key1"], connection_info_1)

    with patch("time.time", return_value=fixed_time + 1800):  # 30 minutes later
        cache.set(additional_cache_keys["key2"], connection_info_2)

    # Check expiry of first entry while second is still valid
    with patch("time.time", return_value=fixed_time + CACHE_EXPIRY_SECONDS):
        assert cache.get(additional_cache_keys["key1"]) is None  # Expired
        assert cache.get(additional_cache_keys["key2"]) is not None  # Still valid

    # Check both are expired after sufficient time
    with patch("time.time", return_value=fixed_time + CACHE_EXPIRY_SECONDS + 1800):
        assert cache.get(additional_cache_keys["key1"]) is None
        assert cache.get(additional_cache_keys["key2"]) is None


def test_cache_set_updates_expiry_time(
    cache, sample_cache_key, sample_connection_info, fixed_time
):
    """Test that setting a value again updates the expiry time."""
    # Set initial value
    with patch("time.time", return_value=fixed_time):
        cache.set(sample_cache_key, sample_connection_info)
        initial_cached = cache.get(sample_cache_key)
        initial_expiry = initial_cached.expiry_time

    # Set same key again later
    with patch("time.time", return_value=fixed_time + 1800):  # 30 minutes later
        cache.set(sample_cache_key, sample_connection_info)
        updated_cached = cache.get(sample_cache_key)
        updated_expiry = updated_cached.expiry_time

    # Expiry time should be updated
    assert updated_expiry > initial_expiry
    assert updated_expiry == fixed_time + 1800 + CACHE_EXPIRY_SECONDS


@mark.parametrize("disable_cache_during_operation", [True, False])
def test_cache_disable_enable_behavior(
    cache, sample_cache_key, sample_connection_info, disable_cache_during_operation
):
    """Test cache behavior when disabled and re-enabled."""
    # Set initial value
    cache.set(sample_cache_key, sample_connection_info)
    assert cache.get(sample_cache_key) is not None

    if disable_cache_during_operation:
        # Disable cache - should return None even if value exists
        cache.disable()
        assert cache.get(sample_cache_key) is None

        # Re-enable cache - should work again
        cache.enable()
        assert cache.get(sample_cache_key) is not None
    else:
        # Keep cache enabled - should continue working
        assert cache.get(sample_cache_key) is not None


def test_helper_functions():
    """Test the backward compatibility helper functions."""
    from tests.unit.test_cache_helpers import cache_token, get_cached_token
    from firebolt.utils.cache import _firebolt_cache
    
    _firebolt_cache.enable()
    _firebolt_cache.clear()
    
    # Test caching and retrieving tokens
    principal = "test_user"
    secret = "test_secret"
    token = "test_token"
    account_name = "test_account"
    
    # Cache token
    cache_token(principal, secret, token, 9999, account_name)
    
    # Retrieve token
    cached_token = get_cached_token(principal, secret, account_name)
    assert cached_token == token
    
    # Test with None account name
    cache_token(principal, secret, token, 9999, None)
    cached_token_none = get_cached_token(principal, secret, None)
    assert cached_token_none == token


def test_connection_info_post_init():
    """Test ConnectionInfo.__post_init__ method."""
    # Test with dictionary inputs that should be converted to dataclasses
    engine_dict = {"url": "http://test.com", "params": {"key": "value"}}
    db_dict = {"name": "test_db"}
    
    connection_info = ConnectionInfo(
        id="test",
        system_engine=engine_dict,
        databases={"db1": db_dict},
        engines={"engine1": engine_dict}
    )
    
    # Should convert dicts to dataclasses
    from firebolt.utils.cache import EngineInfo, DatabaseInfo
    assert isinstance(connection_info.system_engine, EngineInfo)
    assert isinstance(connection_info.databases["db1"], DatabaseInfo)
    assert isinstance(connection_info.engines["engine1"], EngineInfo)
    
    # Test with already converted dataclass objects
    engine_obj = EngineInfo(url="http://test.com", params={"key": "value"})
    db_obj = DatabaseInfo(name="test_db")
    
    connection_info2 = ConnectionInfo(
        id="test2",
        system_engine=engine_obj,
        databases={"db1": db_obj},
        engines={"engine1": engine_obj}
    )
    
    # Should remain as dataclasses
    assert connection_info2.system_engine is engine_obj
    assert connection_info2.databases["db1"] is db_obj
    assert connection_info2.engines["engine1"] is engine_obj
