import time
from typing import Callable, Dict, Generator
from unittest.mock import patch

from httpx import URL
from pytest import fixture, mark
from pytest_httpx import HTTPXMock

from firebolt.async_db import connect
from firebolt.client.auth import Auth
from firebolt.utils.cache import CACHE_EXPIRY_SECONDS, _firebolt_cache


@fixture(autouse=True)
def use_cache(enable_cache) -> Generator[None, None, None]:
    _firebolt_cache.clear()
    yield  # This fixture is used to ensure cache is enabled for all tests by default
    _firebolt_cache.clear()


@fixture
async def connection_test(
    api_endpoint: str,
    auth: Auth,
    account_name: str,
):
    """Fixture to create a connection factory for testing."""

    async def factory(db_name: str, engine_name: str, caching: bool) -> Callable:

        async with await connect(
            database=db_name,
            engine_name=engine_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            disable_cache=not caching,
        ) as connection:
            await connection.cursor().execute("select*")

    return factory


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_caching(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    cache_enabled: bool,
    connection_test: Callable,
):
    system_engine_call_counter = 0
    use_database_call_counter = 0
    use_engine_call_counter = 0

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    def use_database_callback_counter(request, **kwargs):
        nonlocal use_database_call_counter
        use_database_call_counter += 1
        return use_database_callback(request, **kwargs)

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    for _ in range(3):
        await connection_test(db_name, engine_name, cache_enabled)

    if cache_enabled:
        assert system_engine_call_counter == 1, "System engine URL was not cached"
        assert use_database_call_counter == 1, "Use database URL was not cached"
        assert use_engine_call_counter == 1, "Use engine URL was not cached"
    else:
        assert system_engine_call_counter != 1, "System engine URL was cached"
        assert use_database_call_counter != 1, "Use database URL was cached"
        assert use_engine_call_counter != 1, "Use engine URL was cached"

    # Reset caches for the next test iteration
    _firebolt_cache.enable()


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_db_switching_caching(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    cache_enabled: bool,
    connection_test: Callable,
):
    """Test caching when switching between different databases."""
    system_engine_call_counter = 0
    use_database_call_counter = 0
    use_engine_call_counter = 0
    second_db_name = f"{db_name}_second"

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    def use_database_callback_counter(request, **kwargs):
        nonlocal use_database_call_counter
        use_database_call_counter += 1
        return use_database_callback(request, **kwargs)

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )

    # First database
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # Second database
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{second_db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    # Connect to first database
    await connection_test(db_name, engine_name, cache_enabled)

    first_db_calls = use_database_call_counter

    # Connect to second database
    await connection_test(second_db_name, engine_name, cache_enabled)

    second_db_calls = use_database_call_counter - first_db_calls

    # Connect to first database again
    await connection_test(db_name, engine_name, cache_enabled)

    third_db_calls = use_database_call_counter - first_db_calls - second_db_calls

    if cache_enabled:
        assert second_db_calls == 1, "Second database call was not made"
        assert third_db_calls == 0, "First database was not cached"
        assert system_engine_call_counter == 1, "System engine URL was not cached"
        assert use_engine_call_counter == 1, "Use engine URL was not cached"
    else:
        assert second_db_calls == 1, "Second database call was not made"
        assert third_db_calls == 1, "First database was cached when cache disabled"
        assert (
            system_engine_call_counter == 3
        ), "System engine URL was cached when cache disabled"
        assert (
            use_engine_call_counter == 3
        ), "Use engine URL was cached when cache disabled"

    # Reset caches for the next test iteration
    _firebolt_cache.enable()


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_engine_switching_caching(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    cache_enabled: bool,
    connection_test: Callable,
):
    """Test caching when switching between different engines."""
    system_engine_call_counter = 0
    use_database_call_counter = 0
    use_engine_call_counter = 0
    second_engine_name = f"{engine_name}_second"

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    def use_database_callback_counter(request, **kwargs):
        nonlocal use_database_call_counter
        use_database_call_counter += 1
        return use_database_callback(request, **kwargs)

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # First engine
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # Second engine
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{second_engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    # Connect to first engine
    await connection_test(db_name, engine_name, cache_enabled)

    first_engine_calls = use_engine_call_counter

    # Connect to second engine
    await connection_test(db_name, second_engine_name, cache_enabled)

    second_engine_calls = use_engine_call_counter - first_engine_calls

    # Connect to first engine again
    await connection_test(db_name, engine_name, cache_enabled)

    third_engine_calls = (
        use_engine_call_counter - first_engine_calls - second_engine_calls
    )

    if cache_enabled:
        assert second_engine_calls == 1, "Second engine call was not made"
        assert third_engine_calls == 0, "First engine was not cached"
        assert system_engine_call_counter == 1, "System engine URL was not cached"
        assert use_database_call_counter == 1, "Use database URL was not cached"
    else:
        assert second_engine_calls == 1, "Second engine call was not made"
        assert third_engine_calls == 1, "First engine was cached when cache disabled"
        assert (
            system_engine_call_counter == 3
        ), "System engine URL was cached when cache disabled"
        assert (
            use_database_call_counter == 3
        ), "Use database URL was cached when cache disabled"

    # Reset caches for the next test iteration
    _firebolt_cache.enable()


@mark.parametrize("cache_enabled", [True, False])
async def test_connect_db_different_accounts(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: URL,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    cache_enabled: bool,
):
    """Test caching when switching between different databases."""
    system_engine_call_counter = 0
    use_database_call_counter = 0
    use_engine_call_counter = 0

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    def use_database_callback_counter(request, **kwargs):
        nonlocal use_database_call_counter
        use_database_call_counter += 1
        return use_database_callback(request, **kwargs)

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_callback(request, **kwargs)

    get_system_engine_url_new_account = str(get_system_engine_url).replace(
        account_name, account_name + "_second"
    )
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url_new_account,
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    # First connection

    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
        disable_cache=not cache_enabled,
    ) as connection:
        await connection.cursor().execute("select*")

    assert system_engine_call_counter == 1, "System engine URL was not called"
    assert use_engine_call_counter == 1, "Use engine URL was not called"
    assert use_database_call_counter == 1, "Use database URL was not called"

    # Second connection against different account
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name + "_second",
        api_endpoint=api_endpoint,
        disable_cache=not cache_enabled,
    ) as connection:
        await connection.cursor().execute("select*")

    # This should trigger additional calls to the system engine URL and engine/database
    assert (
        system_engine_call_counter == 2
    ), "System engine URL was not called for second account"
    assert (
        use_engine_call_counter == 2
    ), "Use engine URL was not called for second account"
    assert (
        use_database_call_counter == 2
    ), "Use database URL was not called for second account"


async def test_calls_when_cache_expired(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_database_callback: Callable,
    use_engine_callback: Callable,
    query_callback: Callable,
    connection_test: Callable,
):
    """Test that expired cache entries trigger new backend requests."""
    system_engine_call_counter = 0
    use_database_call_counter = 0
    use_engine_call_counter = 0

    def system_engine_callback_counter(request, **kwargs):
        nonlocal system_engine_call_counter
        system_engine_call_counter += 1
        return get_system_engine_callback(request, **kwargs)

    def use_database_callback_counter(request, **kwargs):
        nonlocal use_database_call_counter
        use_database_call_counter += 1
        return use_database_callback(request, **kwargs)

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_callback(request, **kwargs)

    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        system_engine_callback_counter,
        url=get_system_engine_url,
        is_reusable=True,
    )
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    # First connection - should populate cache
    await connection_test(db_name, engine_name, True)  # cache_enabled=True

    # Verify initial calls were made
    assert system_engine_call_counter == 1, "System engine URL was not called initially"
    assert use_database_call_counter == 1, "Use database URL was not called initially"
    assert use_engine_call_counter == 1, "Use engine URL was not called initially"

    # Second connection immediately - should use cache
    await connection_test(db_name, engine_name, True)

    # Verify no additional calls were made (cache hit)
    assert (
        system_engine_call_counter == 1
    ), "System engine URL was called when cache should hit"
    assert (
        use_database_call_counter == 1
    ), "Use database URL was called when cache should hit"
    assert (
        use_engine_call_counter == 1
    ), "Use engine URL was called when cache should hit"

    # Mock time to simulate cache expiry (1 hour + 1 second past current time)
    current_time = int(time.time())
    expired_time = current_time + CACHE_EXPIRY_SECONDS + 1

    with patch("firebolt.utils.cache.time.time", return_value=expired_time):
        # Third connection after cache expiry - should trigger new backend calls
        await connection_test(db_name, engine_name, True)

    # Verify additional calls were made due to cache expiry
    assert (
        system_engine_call_counter == 2
    ), "System engine URL was not called after cache expiry"
    assert (
        use_database_call_counter == 2
    ), "Use database URL was not called after cache expiry"
    assert (
        use_engine_call_counter == 2
    ), "Use engine URL was not called after cache expiry"


async def test_use_engine_parameters_caching(
    db_name: str,
    engine_name: str,
    auth_url: str,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    httpx_mock: HTTPXMock,
    system_engine_no_db_query_url: str,
    system_engine_query_url: str,
    use_database_callback: Callable,
    use_engine_with_params_callback: Callable,
    test_update_parameters: Dict[str, str],
    mock_system_engine_connection_flow: Callable,
):
    """Test that USE ENGINE parameters are cached and correctly retrieved."""
    mock_system_engine_connection_flow()

    use_engine_call_counter = 0

    def use_engine_callback_counter(request, **kwargs):
        nonlocal use_engine_call_counter
        use_engine_call_counter += 1
        return use_engine_with_params_callback(request, **kwargs)

    # Add the missing USE DATABASE callback
    httpx_mock.add_callback(
        use_database_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # Add USE ENGINE callback with parameters
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # First connection - should populate cache with parameters
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        # Verify parameters are set in cursor from USE ENGINE response
        for param_name, expected_value in test_update_parameters.items():
            assert param_name in cursor._set_parameters
            assert cursor._set_parameters[param_name] == expected_value

    # Verify USE ENGINE was called once
    assert use_engine_call_counter == 1, "USE ENGINE was not called on first connection"

    # Second connection - should use cache and not call USE ENGINE again
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        # Verify cached parameters are correctly applied
        for param_name, expected_value in test_update_parameters.items():
            assert param_name in cursor._set_parameters
            assert cursor._set_parameters[param_name] == expected_value

    # Verify USE ENGINE was not called again (cache hit)
    assert use_engine_call_counter == 1, "USE ENGINE was called when cache should hit"


async def test_database_switching_with_same_engine_preserves_database_context(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    query_url: str,
    use_engine_callback: Callable,
    query_callback: Callable,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    dynamic_use_database_callback: Callable,
):
    """
    Test that switching databases with the same engine preserves correct database context.

    This test verifies the bug scenario where:
    1. Connect with database1 + engine1 (cache entry created)
    2. Connect with database2 + engine1 (should add database2 to cache)
    3. Cursors from second connection should have database2, not database1
    """
    second_db_name = f"{db_name}_second"

    # Mock HTTP calls
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        get_system_engine_callback,
        url=get_system_engine_url,
        is_reusable=True,
    )

    # Create dynamic callback for both databases
    use_db_callback = dynamic_use_database_callback(db_name, second_db_name)

    # Add USE DATABASE callbacks for both databases
    httpx_mock.add_callback(
        use_db_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        use_db_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{second_db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # Add USE ENGINE callback
    httpx_mock.add_callback(
        use_engine_callback,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )
    httpx_mock.add_callback(
        query_callback,
        url=query_url,
        is_reusable=True,
    )

    # Also add callback for second database query URL
    second_query_url = query_url.copy_with(
        params={**query_url.params, "database": second_db_name}
    )
    httpx_mock.add_callback(
        query_callback,
        url=second_query_url,
        is_reusable=True,
    )

    # First connection: database1 + engine1
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection1:
        cursor1 = connection1.cursor()
        await cursor1.execute("SELECT 1")

        # Verify first connection has correct database
        assert (
            cursor1.database == db_name
        ), f"First cursor should have database {db_name}"

        # Verify cache contains first database
        cache_record = cursor1.get_cache_record()
        assert cache_record is not None, "Cache should have connection info"
        assert (
            db_name in cache_record.databases
        ), f"Cache should contain database {db_name}"

    # Second connection: database2 + engine1 (same engine)
    async with await connect(
        database=second_db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection2:
        cursor2 = connection2.cursor()
        await cursor2.execute("SELECT 1")

        # Verify second connection has correct database
        assert cursor2.database == second_db_name, (
            f"Second cursor should have database {second_db_name}, "
            f"but has {cursor2.database}. This indicates the database context was overwritten."
        )

        # Verify cache contains both databases
        cache_record = cursor2.get_cache_record()
        assert cache_record is not None, "Cache should have connection info"
        assert (
            db_name in cache_record.databases
        ), f"Cache should still contain database {db_name}"
        assert (
            second_db_name in cache_record.databases
        ), f"Cache should contain database {second_db_name}"

    # Third connection: back to database1 + engine1 (should use cache)
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection3:
        cursor3 = connection3.cursor()
        await cursor3.execute("SELECT 1")

        # Verify third connection has correct database (should be from cache)
        assert cursor3.database == db_name, (
            f"Third cursor should have database {db_name}, "
            f"but has {cursor3.database}. This indicates cached database context is incorrect."
        )


async def test_engine_cache_does_not_contain_database_parameter(
    db_name: str,
    engine_name: str,
    auth_url: str,
    httpx_mock: HTTPXMock,
    check_credentials_callback: Callable,
    get_system_engine_url: str,
    get_system_engine_callback: Callable,
    system_engine_query_url: str,
    system_engine_no_db_query_url: str,
    use_database_callback: Callable,
    use_engine_with_params_callback: Callable,
    query_callback: Callable,
    api_endpoint: str,
    auth: Auth,
    account_name: str,
    test_update_parameters: Dict[str, str],
):
    """
    Test that cached engine parameters do not include database parameter.

    This test verifies the bug scenario where:
    1. Connect with both database and engine defined
    2. Engine cache should not contain 'database' parameter
    3. Only engine-specific parameters should be cached
    """
    # Mock HTTP calls
    httpx_mock.add_callback(check_credentials_callback, url=auth_url, is_reusable=True)
    httpx_mock.add_callback(
        get_system_engine_callback,
        url=get_system_engine_url,
        is_reusable=True,
    )

    # Add USE DATABASE callback
    httpx_mock.add_callback(
        use_database_callback,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # Add USE ENGINE callback that returns parameters (including engine-specific ones)
    httpx_mock.add_callback(
        use_engine_with_params_callback,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
        is_reusable=True,
    )

    # The query callback needs to be more flexible to handle additional parameters
    # in case there's a database parameter
    def flexible_query_callback(request, **kwargs):
        # This will handle queries to the engine with any parameters
        if engine_name in str(request.url) and "SELECT 1" in str(request.content):
            return query_callback(request, **kwargs)

    httpx_mock.add_callback(
        flexible_query_callback,
        is_reusable=True,
    )

    # Create connection with both database and engine
    async with await connect(
        database=db_name,
        engine_name=engine_name,
        auth=auth,
        account_name=account_name,
        api_endpoint=api_endpoint,
    ) as connection:
        cursor = connection.cursor()
        await cursor.execute("SELECT 1")

        # Get the cache record
        cache_record = cursor.get_cache_record()
        assert cache_record is not None, "Cache should have connection info"
        assert (
            engine_name in cache_record.engines
        ), f"Cache should contain engine {engine_name}"

        # Get cached engine info
        cached_engine = cache_record.engines[engine_name]

        # Verify that engine cache contains expected parameters from test_update_parameters
        for param_name, expected_value in test_update_parameters.items():
            assert (
                param_name in cached_engine.params
            ), f"Engine cache should contain parameter {param_name}"
            assert cached_engine.params[param_name] == expected_value, (
                f"Engine cache parameter {param_name} should be {expected_value}, "
                f"but is {cached_engine.params[param_name]}"
            )

        # Verify that engine cache does NOT contain database parameter
        assert "database" not in cached_engine.params, (
            f"Engine cache should NOT contain 'database' parameter, "
            f"but found database={cached_engine.params.get('database')}. "
            f"Engine parameters should be separate from database context. "
            f"Full engine params: {cached_engine.params}"
        )
