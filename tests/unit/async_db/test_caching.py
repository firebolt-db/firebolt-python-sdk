from typing import Callable

from pytest import fixture, mark
from pytest_httpx import HTTPXMock

from firebolt.async_db import connect
from firebolt.client.auth import Auth
from firebolt.common.cache import _firebolt_cache


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


@fixture(autouse=True)
async def enable_cache():
    _firebolt_cache.enable()
    _firebolt_cache.clear()
    yield
    _firebolt_cache.clear()


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

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(system_engine_callback_counter, url=get_system_engine_url)
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )

    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
    )
    httpx_mock.add_callback(query_callback, url=query_url)

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

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(system_engine_callback_counter, url=get_system_engine_url)

    # First database
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )

    # Second database
    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{second_db_name}"'.encode("utf-8"),
    )

    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
    )
    httpx_mock.add_callback(query_callback, url=query_url)

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

    httpx_mock.add_callback(check_credentials_callback, url=auth_url)
    httpx_mock.add_callback(system_engine_callback_counter, url=get_system_engine_url)

    httpx_mock.add_callback(
        use_database_callback_counter,
        url=system_engine_no_db_query_url,
        match_content=f'USE DATABASE "{db_name}"'.encode("utf-8"),
    )

    # First engine
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{engine_name}"'.encode("utf-8"),
    )

    # Second engine
    httpx_mock.add_callback(
        use_engine_callback_counter,
        url=system_engine_query_url,
        match_content=f'USE ENGINE "{second_engine_name}"'.encode("utf-8"),
    )

    httpx_mock.add_callback(query_callback, url=query_url)

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
