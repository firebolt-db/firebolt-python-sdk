from httpx import Response

from firebolt.common.urls import DATABASES_URL, ENGINES_URL


async def is_db_available(connection, database) -> bool:
    """Verify if the database exists"""
    resp = await _filter_request(
        connection, DATABASES_URL, {"filter.name_contains": database}
    )
    resp.raise_for_status()
    return len(resp.json()["edges"]) > 0


async def is_engine_running(connection, engine) -> bool:
    """Verify if the engine is running"""
    resp = await _filter_request(
        connection,
        ENGINES_URL,
        {
            "filter.name_contains": engine,
            "filter.current_status_eq": "ENGINE_STATUS_RUNNING",
        },
    )
    resp.raise_for_status()
    return len(resp.json()["edges"]) > 0


async def _filter_request(connection, endpoint: str, filters: dict) -> Response:
    resp = await connection._client.request(
        # Full URL overrides the client url, which contains engine as a prefix
        url=connection.api_endpoint + endpoint,
        method="GET",
        params=filters,
    )
    return resp
