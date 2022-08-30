from __future__ import annotations

from typing import TYPE_CHECKING

from httpx import URL, Response

from firebolt.utils.urls import DATABASES_URL, ENGINES_URL

if TYPE_CHECKING:
    from firebolt.async_db.connection import Connection


async def is_db_available(connection: Connection, database_name: str) -> bool:
    """
    Verify that the database exists.

    Args:
        connection (firebolt.async_db.connection.Connection)
    """
    resp = await _filter_request(
        connection, DATABASES_URL, {"filter.name_contains": database_name}
    )
    return len(resp.json()["edges"]) > 0


async def is_engine_running(connection: Connection, engine_url: str) -> bool:
    """
    Verify that the engine is running.

    Args:
        connection (firebolt.async_db.connection.Connection): connection.
    """
    # Url is not guaranteed to be of this structure,
    # but for the sake of error checking this is sufficient.
    engine_name = URL(engine_url).host.split(".")[0]
    resp = await _filter_request(
        connection,
        ENGINES_URL,
        {
            "filter.name_contains": engine_name,
            "filter.current_status_eq": "ENGINE_STATUS_RUNNING",
        },
    )
    return len(resp.json()["edges"]) > 0


async def _filter_request(
    connection: Connection, endpoint: str, filters: dict
) -> Response:
    resp = await connection._client.request(
        # Full url overrides the client url, which contains engine as a prefix.
        url=connection.api_endpoint + endpoint,
        method="GET",
        params=filters,
    )
    resp.raise_for_status()
    return resp
