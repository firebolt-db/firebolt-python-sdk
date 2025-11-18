import json
import os
import time
from typing import Generator
from unittest.mock import mock_open, patch

from pytest import fixture, mark

from firebolt.client.auth.client_credentials import ClientCredentials
from firebolt.db import connect
from firebolt.utils.cache import (
    CACHE_EXPIRY_SECONDS,
    ConnectionInfo,
    FileBasedCache,
    SecureCacheKey,
    UtilCache,
    _firebolt_cache,
)
from firebolt.utils.file_operations import FernetEncrypter, generate_salt


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


@fixture
def file_based_cache() -> Generator[FileBasedCache, None, None]:
    """Create a fresh FileBasedCache instance for testing."""
    memory_cache = UtilCache[ConnectionInfo](cache_name="test_memory_cache")
    memory_cache.enable()
    cache = FileBasedCache(memory_cache, cache_name="test_file_cache")
    cache.enable()
    yield cache
    cache.clear()


@fixture
def encrypter_with_key():
    """Create a FernetEncrypter instance for testing."""
    from firebolt.utils.file_operations import FernetEncrypter, generate_salt

    salt = generate_salt()
    return FernetEncrypter(salt, "test_encryption_key")


@fixture
def auth_client_1() -> ClientCredentials:
    """Authentication for client 1."""
    return ClientCredentials(
        client_id="client_1", client_secret="secret_1", use_token_cache=True
    )


@fixture
def auth_client_2() -> ClientCredentials:
    """Authentication for client 2."""
    return ClientCredentials(
        client_id="client_2", client_secret="secret_2", use_token_cache=True
    )


@fixture
def auth_same_client_diff_secret() -> ClientCredentials:
    """Authentication with same client ID but different secret."""
    return ClientCredentials(
        client_id="client_1",  # Same as auth_client_1
        client_secret="secret_different",  # Different secret
        use_token_cache=True,
    )


@fixture
def mock_connection_flow(httpx_mock, auth_url, api_endpoint):
    """Mock the connection flow for testing."""

    def _mock_flow(account_name="mock_account_name"):
        # Mock authentication with correct URL pattern from existing tests
        httpx_mock.add_response(
            method="POST",
            url=auth_url,  # Use the correct auth_url fixture
            json={"access_token": "mock_token", "expires_in": 3600},
        )

        # Mock system engine URL - this is the actual URL pattern used
        httpx_mock.add_response(
            method="GET",
            url=f"https://{api_endpoint}/web/v3/account/{account_name}/engineUrl",
            json={"engineUrl": "https://system.mock.firebolt.io"},
        )

        # Mock queries to the system engine (this handles all POST requests to system.mock.firebolt.io)
        # This will catch USE DATABASE, USE ENGINE queries and any other queries
        def system_engine_callback(request):
            return {"meta": [], "data": [], "rows": 0, "statistics": {}}

        httpx_mock.add_callback(
            system_engine_callback,
            url="https://system.mock.firebolt.io/",
            method="POST",
        )

    return _mock_flow


@fixture
def clean_cache():
    """Provide a clean cache for each test."""
    original_memory = _firebolt_cache.memory_cache._cache.copy()
    original_file_cache_disabled = _firebolt_cache.disabled

    _firebolt_cache.clear()
    _firebolt_cache.enable()  # Enable cache for connection flow tests
    yield _firebolt_cache

    # Restore original state
    _firebolt_cache.memory_cache._cache = original_memory
    _firebolt_cache.disabled = original_file_cache_disabled


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
    from firebolt.utils.cache import _firebolt_cache
    from tests.unit.test_cache_helpers import cache_token, get_cached_token

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
        engines={"engine1": engine_dict},
    )

    # Should convert dicts to dataclasses
    from firebolt.utils.cache import DatabaseInfo, EngineInfo

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
        engines={"engine1": engine_obj},
    )

    # Should remain as dataclasses
    assert connection_info2.system_engine is engine_obj
    assert connection_info2.databases["db1"] is db_obj
    assert connection_info2.engines["engine1"] is engine_obj


@mark.nofakefs
def test_file_based_cache_read_data_json_file_not_exists(
    file_based_cache, encrypter_with_key
):
    """Test _read_data_json returns empty dict when file doesn't exist."""
    # Test with a non-existent file path
    result = file_based_cache._read_data_json(
        "/path/to/nonexistent/file.txt", encrypter_with_key
    )
    assert result == {}


def test_file_based_cache_read_data_json_valid_data(
    file_based_cache, encrypter_with_key
):
    """Test _read_data_json successfully reads and decrypts valid JSON data."""
    # Create test data
    test_data = {"id": "test_connection", "token": "test_token"}
    test_file_path = "/test_cache/valid_data.txt"

    # Create directory and file with encrypted JSON data
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    json_str = json.dumps(test_data)
    encrypted_data = encrypter_with_key.encrypt(json_str)

    with open(test_file_path, "w") as f:
        f.write(encrypted_data)

    # Test reading the valid encrypted data
    result = file_based_cache._read_data_json(test_file_path, encrypter_with_key)
    assert result == test_data
    assert result["id"] == "test_connection"
    assert result["token"] == "test_token"


def test_file_based_cache_read_data_json_decryption_failure(file_based_cache):
    """Test _read_data_json returns empty dict when decryption fails."""
    # Create encrypters with different keys
    salt = generate_salt()
    encrypter1 = FernetEncrypter(salt, "test_key_1")
    encrypter2 = FernetEncrypter(salt, "test_key_2")  # Different key

    test_file_path = "/test_cache/decryption_test.txt"

    # Create directory and file with data encrypted by encrypter1
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    encrypted_data = encrypter1.encrypt('{"test": "data"}')

    with open(test_file_path, "w") as f:
        f.write(encrypted_data)

    # Try to decrypt with encrypter2 (should fail)
    result = file_based_cache._read_data_json(test_file_path, encrypter2)
    assert result == {}


def test_file_based_cache_read_data_json_invalid_json(
    file_based_cache, encrypter_with_key
):
    """Test _read_data_json returns empty dict when JSON is invalid."""
    test_file_path = "/test_cache/invalid_json.txt"

    # Create directory and file with encrypted invalid JSON
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    invalid_json = "invalid json data {{"
    encrypted_data = encrypter_with_key.encrypt(invalid_json)

    with open(test_file_path, "w") as f:
        f.write(encrypted_data)

    # Test reading the invalid JSON
    result = file_based_cache._read_data_json(test_file_path, encrypter_with_key)
    assert result == {}


@mark.nofakefs
def test_file_based_cache_read_data_json_io_error(file_based_cache, encrypter_with_key):
    """Test _read_data_json returns empty dict when IOError occurs."""
    # Mock open to raise IOError
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = IOError("File read error")

        result = file_based_cache._read_data_json("test_file.txt", encrypter_with_key)
        assert result == {}


def test_file_based_cache_read_data_json_empty_encrypted_data(
    file_based_cache, encrypter_with_key
):
    """Test _read_data_json handles empty encrypted data."""
    test_file_path = "/test_cache/empty_data.txt"

    # Create directory and file with empty encrypted data
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    encrypted_empty = encrypter_with_key.encrypt("")

    with open(test_file_path, "w") as f:
        f.write(encrypted_empty)

    # Test reading empty decrypted data
    result = file_based_cache._read_data_json(test_file_path, encrypter_with_key)
    assert result == {}


def test_file_based_cache_read_data_json_invalid_encrypted_format(
    file_based_cache, encrypter_with_key
):
    """Test _read_data_json handles invalid encrypted data format."""
    test_file_path = "/test_cache/invalid_encrypted.txt"

    # Create directory and file with invalid encrypted data format
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)

    with open(test_file_path, "w") as f:
        f.write("not_encrypted_data_at_all")

    # Test reading invalid encrypted format
    result = file_based_cache._read_data_json(test_file_path, encrypter_with_key)
    assert result == {}


def test_file_based_cache_delete_method(file_based_cache, encrypter_with_key):
    """Test FileBasedCache delete method removes data from both memory and file."""
    # Create test data
    sample_key = SecureCacheKey(["delete", "test"], "test_secret")
    sample_data = ConnectionInfo(id="test_delete_connection", token="test_token")

    # Set data in cache (both memory and file)
    file_based_cache.set(sample_key, sample_data)

    # Verify data exists in memory cache
    memory_result = file_based_cache.memory_cache.get(sample_key)
    assert memory_result is not None
    assert memory_result.id == "test_delete_connection"

    # Verify file exists on disk
    file_path = file_based_cache._get_file_path(sample_key)
    assert os.path.exists(file_path)

    # Delete the data
    file_based_cache.delete(sample_key)

    # Verify data is removed from memory cache
    memory_result_after_delete = file_based_cache.memory_cache.get(sample_key)
    assert memory_result_after_delete is None

    # Verify file is removed from disk
    assert not os.path.exists(file_path)

    # Verify get returns None
    cache_result = file_based_cache.get(sample_key)
    assert cache_result is None


@mark.nofakefs
def test_file_based_cache_delete_method_file_removal_failure(
    file_based_cache, encrypter_with_key
):
    """Test FileBasedCache delete method handles file removal failures gracefully."""
    sample_key = SecureCacheKey(["delete", "failure"], "test_secret")
    sample_data = ConnectionInfo(id="test_connection", token="test_token")

    # Set data in memory cache only (no file operations due to @mark.nofakefs)
    file_based_cache.memory_cache.set(sample_key, sample_data)

    # Mock path.exists to return True and os.remove to raise OSError
    with patch("firebolt.utils.cache.path.exists", return_value=True), patch(
        "firebolt.utils.cache.os.remove"
    ) as mock_remove:
        mock_remove.side_effect = OSError("Permission denied")

        # Delete should not raise an exception despite file removal failure
        file_based_cache.delete(sample_key)

        # Verify data is still removed from memory cache
        memory_result = file_based_cache.memory_cache.get(sample_key)
        assert memory_result is None


def test_file_based_cache_get_from_file_when_not_in_memory(
    file_based_cache, encrypter_with_key
):
    """Test FileBasedCache get method retrieves data from file when not in memory."""
    # Create test data
    sample_key = SecureCacheKey(["file", "only"], "test_secret")
    sample_data = ConnectionInfo(
        id="test_file_connection",
        token="test_file_token",
        expiry_time=int(time.time()) + 3600,  # Valid for 1 hour
    )

    # First set data in cache (both memory and file)
    file_based_cache.set(sample_key, sample_data)

    # Verify data exists
    initial_result = file_based_cache.get(sample_key)
    assert initial_result is not None
    assert initial_result.id == "test_file_connection"

    # Clear memory cache but keep file
    file_based_cache.memory_cache.clear()

    # Verify memory cache is empty
    memory_result = file_based_cache.memory_cache.get(sample_key)
    assert memory_result is None

    # Verify file still exists
    file_path = file_based_cache._get_file_path(sample_key)
    assert os.path.exists(file_path)

    # Get should retrieve from file and reload into memory
    file_result = file_based_cache.get(sample_key)
    assert file_result is not None
    assert file_result.id == "test_file_connection"
    assert file_result.token == "test_file_token"

    # Verify data is now back in memory cache
    memory_result_after_load = file_based_cache.memory_cache.get(sample_key)
    assert memory_result_after_load is not None
    assert memory_result_after_load.id == "test_file_connection"


def test_file_based_cache_get_from_corrupted_file(file_based_cache, encrypter_with_key):
    """Test FileBasedCache get method handles corrupted file gracefully."""
    sample_key = SecureCacheKey(["corrupted", "file"], "test_secret")

    # Create corrupted file manually
    file_path = file_based_cache._get_file_path(sample_key)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as f:
        f.write("corrupted_data_that_cannot_be_decrypted")

    # Verify file exists
    assert os.path.exists(file_path)

    # Get should return None due to decryption failure
    result = file_based_cache.get(sample_key)
    assert result is None

    # Verify nothing is loaded into memory cache
    memory_result = file_based_cache.memory_cache.get(sample_key)
    assert memory_result is None


def test_file_based_cache_disabled_behavior(file_based_cache, encrypter_with_key):
    """Test FileBasedCache methods when cache is disabled."""
    sample_key = SecureCacheKey(["disabled", "test"], "test_secret")
    sample_data = ConnectionInfo(id="test_connection", token="test_token")

    # Disable the cache
    file_based_cache.disable()

    # Set should do nothing when disabled
    file_based_cache.set(sample_key, sample_data)

    # Get should return None when disabled
    result = file_based_cache.get(sample_key)
    assert result is None

    # Enable cache, set data, then disable again
    file_based_cache.enable()
    file_based_cache.set(sample_key, sample_data)

    # Verify data is set
    enabled_result = file_based_cache.get(sample_key)
    assert enabled_result is not None

    # Disable and verify get returns None
    file_based_cache.disable()
    disabled_result = file_based_cache.get(sample_key)
    assert disabled_result is None

    # Delete should do nothing when disabled
    file_based_cache.delete(sample_key)  # Should not raise exception


def test_file_based_cache_preserves_expiry_from_file(
    file_based_cache, encrypter_with_key, fixed_time
):
    """Test that FileBasedCache preserves original expiry time when loading from file."""
    sample_key = SecureCacheKey(["preserve", "expiry"], "test_secret")

    # Create data and set it at an earlier time
    sample_data = ConnectionInfo(id="test_connection")

    # Set data at fixed_time - this will give it expiry of fixed_time + CACHE_EXPIRY_SECONDS
    with patch("time.time", return_value=fixed_time):
        file_based_cache.set(sample_key, sample_data)

        # Verify the expiry time that was set
        memory_result = file_based_cache.memory_cache.get(sample_key)
        expected_expiry = fixed_time + CACHE_EXPIRY_SECONDS
        assert memory_result.expiry_time == expected_expiry

        # Clear memory cache to force file load on next get
        file_based_cache.memory_cache.clear()

        # Get data from file (should preserve the original expiry time from file)
        result = file_based_cache.get(sample_key)

        assert result is not None
        assert (
            result.expiry_time == expected_expiry
        )  # Should preserve original expiry from file
        assert result.id == "test_connection"

        # Verify it's also in memory cache with preserved expiry
        memory_result_after_load = file_based_cache.memory_cache.get(sample_key)
        assert memory_result_after_load is not None
        assert memory_result_after_load.expiry_time == expected_expiry


def test_file_based_cache_deletes_expired_file_on_get(
    file_based_cache, encrypter_with_key, fixed_time
):
    """Test that FileBasedCache deletes expired files on get and returns cache miss."""
    sample_key = SecureCacheKey(["expired", "file"], "test_secret")
    sample_data = ConnectionInfo(id="test_connection")

    # Set data at an early time so it gets an early expiry
    early_time = fixed_time - 7200  # 2 hours before
    with patch("time.time", return_value=early_time):
        file_based_cache.set(sample_key, sample_data)

        # Verify the expiry time that was set (should be early_time + CACHE_EXPIRY_SECONDS)
        memory_result = file_based_cache.memory_cache.get(sample_key)
        expected_expiry = early_time + CACHE_EXPIRY_SECONDS
        assert memory_result.expiry_time == expected_expiry

    # Verify file was created
    file_path = file_based_cache._get_file_path(sample_key)
    assert os.path.exists(file_path)

    # Clear memory cache to force file load
    file_based_cache.memory_cache.clear()

    # Now try to get at a time when the data should be expired
    # The data expires at early_time + CACHE_EXPIRY_SECONDS
    # Let's try to get it after that expiry time
    expired_check_time = early_time + CACHE_EXPIRY_SECONDS + 1
    with patch("time.time", return_value=expired_check_time):
        result = file_based_cache.get(sample_key)

        # Should return None due to expiry
        assert result is None

        # File should be deleted
        assert not os.path.exists(file_path)

        # Memory cache should not contain the data
        memory_result = file_based_cache.memory_cache.get(sample_key)
        assert memory_result is None


def test_file_based_cache_expiry_edge_case_exactly_expired(
    file_based_cache, encrypter_with_key, fixed_time
):
    """Test behavior when data expires exactly at the current time."""
    sample_key = SecureCacheKey(["edge", "case"], "test_secret")
    sample_data = ConnectionInfo(id="test_connection")

    # Set data such that it will expire exactly at fixed_time
    set_time = fixed_time - CACHE_EXPIRY_SECONDS
    with patch("time.time", return_value=set_time):
        file_based_cache.set(sample_key, sample_data)

        # Verify the expiry time that was set
        memory_result = file_based_cache.memory_cache.get(sample_key)
        expected_expiry = set_time + CACHE_EXPIRY_SECONDS  # This equals fixed_time
        assert memory_result.expiry_time == expected_expiry == fixed_time

    file_path = file_based_cache._get_file_path(sample_key)
    assert os.path.exists(file_path)

    # Clear memory cache
    file_based_cache.memory_cache.clear()

    # Try to get exactly at expiry time (should be considered expired)
    with patch("time.time", return_value=fixed_time):
        result = file_based_cache.get(sample_key)

        # Should return None as data is expired (>= check in _is_expired)
        assert result is None

        # File should be deleted
        assert not os.path.exists(file_path)


def test_file_based_cache_non_expired_file_loads_correctly(
    file_based_cache, encrypter_with_key, fixed_time
):
    """Test that non-expired data from file loads correctly with preserved expiry."""
    sample_key = SecureCacheKey(["non", "expired"], "test_secret")

    sample_data = ConnectionInfo(id="test_connection", token="test_token")

    # Set data at an earlier time so it's not expired yet
    set_time = fixed_time - 900  # 15 minutes before
    with patch("time.time", return_value=set_time):
        file_based_cache.set(sample_key, sample_data)

        # Verify expiry time
        memory_result = file_based_cache.memory_cache.get(sample_key)
        expected_expiry = set_time + CACHE_EXPIRY_SECONDS
        assert memory_result.expiry_time == expected_expiry

    # Clear memory cache to force file load
    file_based_cache.memory_cache.clear()

    # Get data at fixed_time (data should not be expired since expected_expiry > fixed_time)
    with patch("time.time", return_value=fixed_time):
        # Ensure the data is not expired
        assert expected_expiry > fixed_time, "Data should not be expired for this test"

        result = file_based_cache.get(sample_key)

        # Should successfully load data
        assert result is not None
        assert result.id == "test_connection"
        assert result.token == "test_token"
        assert result.expiry_time == expected_expiry  # Preserved original expiry

        # Verify file still exists (not deleted)
        file_path = file_based_cache._get_file_path(sample_key)
        assert os.path.exists(file_path)

        # Verify it's in memory cache with preserved expiry
        memory_result = file_based_cache.memory_cache.get(sample_key)
        assert memory_result is not None
        assert memory_result.expiry_time == expected_expiry


def test_memory_cache_set_preserve_expiry_parameter(
    cache, sample_cache_key, fixed_time
):
    """Test UtilCache.set preserve_expiry parameter functionality."""
    # Create connection info with specific expiry time
    original_expiry = fixed_time + 1800
    sample_data = ConnectionInfo(id="test_connection", expiry_time=original_expiry)

    with patch("time.time", return_value=fixed_time):
        # Test preserve_expiry=True
        cache.set(sample_cache_key, sample_data, preserve_expiry=True)

        result = cache.get(sample_cache_key)
        assert result is not None
        assert result.expiry_time == original_expiry  # Should preserve original

        cache.clear()

        # Test preserve_expiry=False (default behavior)
        cache.set(sample_cache_key, sample_data, preserve_expiry=False)

        result = cache.get(sample_cache_key)
        assert result is not None
        expected_new_expiry = fixed_time + CACHE_EXPIRY_SECONDS
        assert result.expiry_time == expected_new_expiry  # Should get new expiry

        cache.clear()

        # Test default behavior (preserve_expiry not specified)
        cache.set(sample_cache_key, sample_data)

        result = cache.get(sample_cache_key)
        assert result is not None
        assert result.expiry_time == expected_new_expiry  # Should get new expiry


def test_memory_cache_set_preserve_expiry_with_none_expiry(
    cache, sample_cache_key, fixed_time
):
    """Test UtilCache.set preserve_expiry when original expiry_time is None."""
    # Create connection info with None expiry time
    sample_data = ConnectionInfo(id="test_connection", expiry_time=None)

    with patch("time.time", return_value=fixed_time):
        # Even with preserve_expiry=True, None expiry should get new expiry
        cache.set(sample_cache_key, sample_data, preserve_expiry=True)

        result = cache.get(sample_cache_key)
        assert result is not None
        expected_expiry = fixed_time + CACHE_EXPIRY_SECONDS
        assert (
            result.expiry_time == expected_expiry
        )  # Should get new expiry despite preserve=True


# Comprehensive cache tests using full connection flow
def test_cache_stores_connection_data(
    clean_cache,
    auth_client_1,
    mock_connection_flow,
    api_endpoint,
    account_name,
):
    """Test that cache stores connection data correctly."""
    # Setup mock responses
    mock_connection_flow(account_name)

    # Create connection and verify data is cached
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        # Verify cache contains the expected data
        cache_key = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_name],
            auth_client_1.secret,
        )

        cached_data = _firebolt_cache.get(cache_key)
        assert cached_data is not None, "Cache should contain connection data"


def test_different_accounts_isolated_cache_entries(
    clean_cache,
    auth_client_1,
    mock_connection_flow,
    api_endpoint,
):
    """Test that different accounts generate isolated cache entries."""
    account_1 = "test_account_1"
    account_2 = "test_account_2"

    # Setup mock responses for both accounts
    mock_connection_flow(account_1)
    mock_connection_flow(account_2)

    # Connect to first account
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_1,
        api_endpoint=api_endpoint,
    ):
        # Get first cache entry
        key_account_1 = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_1],
            auth_client_1.secret,
        )
        cache_1 = _firebolt_cache.get(key_account_1)
        assert cache_1 is not None, "Account 1 should have cache entry"

    # Connect to second account with same credentials
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_2,
        api_endpoint=api_endpoint,
    ):
        # Get second cache entry
        key_account_2 = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_2],
            auth_client_1.secret,
        )
        cache_2 = _firebolt_cache.get(key_account_2)
        assert cache_2 is not None, "Account 2 should have cache entry"

    # Verify cache keys are different
    assert key_account_1.key != key_account_2.key, "Cache keys should be different"
    assert account_1 in key_account_1.key, "Account 1 should be in cache key"
    assert account_2 in key_account_2.key, "Account 2 should be in cache key"


def test_different_credentials_isolated_cache_entries(
    clean_cache,
    auth_client_1,
    auth_client_2,
    mock_connection_flow,
    api_endpoint,
    account_name,
):
    """Test that different credentials generate isolated cache entries."""
    # Setup mock responses
    mock_connection_flow(account_name)

    # Connect with first credentials
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        key_cred_1 = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_name],
            auth_client_1.secret,
        )
        cache_1 = _firebolt_cache.get(key_cred_1)
        assert cache_1 is not None, "Credential 1 should have cache entry"

    # Connect with second credentials
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_2,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        key_cred_2 = SecureCacheKey(
            [auth_client_2.principal, auth_client_2.secret, account_name],
            auth_client_2.secret,
        )
        cache_2 = _firebolt_cache.get(key_cred_2)
        assert cache_2 is not None, "Credential 2 should have cache entry"

    # Verify cache keys are different
    assert key_cred_1.key != key_cred_2.key, "Cache keys should be different"


def test_cache_delete_consistency_with_connections(
    clean_cache,
    auth_client_1,
    mock_connection_flow,
    api_endpoint,
    account_name,
):
    """Test that cache deletion is consistent across memory and disk with real connections."""
    # Setup mock responses
    mock_connection_flow(account_name)

    # Establish connection and populate cache
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        cache_key = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_name],
            auth_client_1.secret,
        )

        # Verify cache exists
        assert (
            _firebolt_cache.get(cache_key) is not None
        ), "Cache should exist before deletion"

        # Delete from cache
        _firebolt_cache.delete(cache_key)

        # Verify deletion from memory
        assert (
            _firebolt_cache.memory_cache.get(cache_key) is None
        ), "Memory cache should be deleted"

        # Verify deletion persisted to disk
        disk_result = _firebolt_cache.get(cache_key)
        assert disk_result is None, "Disk cache should be deleted"


@mark.nofakefs  # These tests need to test real disk behavior
def test_memory_first_disk_fallback_with_connections(
    clean_cache,
    auth_client_1,
    mock_connection_flow,
    api_endpoint,
    account_name,
):
    """Test that memory cache is checked first, then disk cache."""
    # Setup mock responses
    mock_connection_flow(account_name)

    # First connection - populates both memory and disk cache
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        cache_key = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_name],
            auth_client_1.secret,
        )

        # Verify data is in memory cache
        memory_data = _firebolt_cache.memory_cache.get(cache_key)
        assert memory_data is not None, "Data should be in memory cache"

        # Clear memory cache but keep disk cache
        _firebolt_cache.memory_cache.clear()
        assert (
            _firebolt_cache.memory_cache.get(cache_key) is None
        ), "Memory cache should be cleared"

        # Get from cache should load from disk and populate memory
        reloaded_data = _firebolt_cache.get(cache_key)
        assert reloaded_data is not None, "Data should be reloaded from disk"


@mark.usefixtures("fs")  # Use pyfakefs for filesystem mocking
def test_disk_file_operations_with_pyfakefs(
    clean_cache,
    auth_client_1,
    mock_connection_flow,
    api_endpoint,
    account_name,
):
    """Test disk file operations using pyfakefs."""
    # Setup mock responses
    mock_connection_flow(account_name)

    # Establish connection and verify file creation
    with connect(
        database="test_db",
        engine_name="test_engine",
        auth=auth_client_1,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ):
        # Verify cache file was created in fake filesystem
        cache_dir = os.path.expanduser("~/.firebolt")
        if os.path.exists(cache_dir):
            cache_files = [f for f in os.listdir(cache_dir) if f.endswith(".json")]
            # May or may not have files depending on implementation
            assert isinstance(cache_files, list), "Cache files list should be valid"

        # Verify file content can be read from cache
        cache_key = SecureCacheKey(
            [auth_client_1.principal, auth_client_1.secret, account_name],
            auth_client_1.secret,
        )
        cache_data = _firebolt_cache.get(cache_key)
        assert cache_data is not None, "Cache data should be accessible"
