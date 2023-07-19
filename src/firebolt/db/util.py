from __future__ import annotations

from typing import TYPE_CHECKING, Tuple

from httpx import URL, Timeout, codes

from firebolt.client import Client
from firebolt.client.auth import Auth
from firebolt.common.settings import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    AccountNotFoundError,
    FireboltEngineError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME

if TYPE_CHECKING:
    from firebolt.db.connection import Connection

ENGINE_STATUS_RUNNING = "Running"


def is_db_available(connection: Connection, database_name: str) -> bool:
    """
    Verify that the database exists.

    Args:
        connection (firebolt.db.connection.Connection)
        database_name (str): Name of a database
    """
    system_engine = connection._system_engine_connection or connection
    with system_engine.cursor() as cursor:
        return (
            cursor.execute(
                """
                SELECT 1 FROM information_schema.databases WHERE database_name=?
                """,
                [database_name],
            )
            > 0
        )


def is_engine_running(connection: Connection, engine_url: str) -> bool:
    """
    Verify that the engine is running.

    Args:
        connection (firebolt.db.connection.Connection): connection.
        engine_url (str): URL of the engine
    """

    if connection._is_system:
        # System engine is always running
        return True

    engine_name = URL(engine_url).host.split(".")[0].replace("-", "_")
    assert connection._system_engine_connection is not None  # Type check
    _, status, _ = _get_engine_url_status_db(
        connection._system_engine_connection, engine_name
    )
    return status == ENGINE_STATUS_RUNNING


def _get_system_engine_url(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> str:
    with Client(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        url = GATEWAY_HOST_BY_ACCOUNT_NAME.format(account_name=account_name)
        response = client.get(url=url)
        if response.status_code == codes.NOT_FOUND:
            raise AccountNotFoundError(account_name)
        if response.status_code != codes.OK:
            raise InterfaceError(
                f"Unable to retrieve system engine endpoint {url}: "
                f"{response.status_code} {response.content}"
            )
        return response.json()["engineUrl"]


def _get_engine_url_status_db(
    system_engine: Connection, engine_name: str
) -> Tuple[str, str, str]:
    with system_engine.cursor() as cursor:
        cursor.execute(
            """
            SELECT url, attached_to, status FROM information_schema.engines
            WHERE engine_name=?
            """,
            [engine_name],
        )
        row = cursor.fetchone()
        if row is None:
            raise FireboltEngineError(f"Engine with name {engine_name} doesn't exist")
        engine_url, database, status = row
        return str(engine_url), str(status), str(database)  # Mypy check
