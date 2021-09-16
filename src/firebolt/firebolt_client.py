from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from types import TracebackType
from typing import Optional, Type

from firebolt import client
from firebolt.common.exception import FireboltClientRequiredError
from firebolt.common.settings import Settings
from firebolt.http_client import get_access_token, get_http_client

logger = logging.getLogger(__name__)

_firebolt_client_singleton: Optional[client.FireboltClient] = None


def get_firebolt_client() -> FireboltClient:
    """
    Get the initialized FireboltClient singleton.

    This function is intended to be used by classes and functions that will
    be accessed within an existing FireboltClient context.
    """
    global _firebolt_client_singleton
    if _firebolt_client_singleton is None:
        raise FireboltClientRequiredError()
    return _firebolt_client_singleton


@contextmanager
def init_firebolt_client(settings: Optional[Settings]) -> client.FireboltClient:
    global _firebolt_client_singleton
    settings = settings or Settings()
    _firebolt_client_singleton = client.FireboltClient(
        auth=(settings.user, settings.password),
        base_url=settings.server,
        api_endpoint=settings.server,
    )
    yield _firebolt_client_singleton
    _firebolt_client_singleton.close()
    _firebolt_client_singleton = None


class FireboltClient:
    """
    Client for interacting with Firebolt.

    This class is intended to be used as a context manager to ensure connections to
    Firebolt are closed upon exit. For example:
    >>> with FireboltClient():
    >>>     ...
    """

    def __init__(self, settings: Optional[Settings] = None):
        if settings is None:
            settings = Settings()
        self.settings = settings

        self.access_token = get_access_token(
            host=self.settings.server,
            username=self.settings.user,
            password=self.settings.password,
        )
        self.http_client = get_http_client(
            host=self.settings.server, access_token=self.access_token
        )
        logger.info(f"Connected to {self.settings.server} as {self.settings.user}")

        self.default_region_name = self.settings.default_region
        self.default_provider_name = self.settings.default_provider

    def __enter__(self) -> FireboltClient:
        global _firebolt_client_singleton
        _firebolt_client_singleton = self
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.http_client.close()
        logger.info(f"Connection to {self.settings.server} closed")
        global _firebolt_client_singleton
        _firebolt_client_singleton = None

    @cached_property
    def account_id(self) -> str:
        response = self.http_client.get(
            url="/iam/v2/account",
        )
        account_id = response.json()["account"]["id"]
        return account_id
