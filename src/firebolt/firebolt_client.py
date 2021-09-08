from __future__ import annotations

import logging
from functools import cached_property
from types import TracebackType
from typing import Optional, Type

import dotenv

from firebolt.common import env
from firebolt.common.exception import FireboltClientRequiredError
from firebolt.http_client import get_access_token, get_http_client

logger = logging.getLogger(__name__)

_firebolt_client_singleton: Optional[FireboltClient] = None


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


class FireboltClient:
    """
    Client for interacting with Firebolt.

    This class is intended to be used as a context manager to ensure connections to
    Firebolt are closed upon exit. For example:
    >>> with FireboltClient.from_env():
    >>>     ...
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        default_region_name: Optional[str] = None,
        default_provider_name: Optional[str] = None,
    ):
        self.username = username
        self.password = password
        self.host = host
        self.access_token = get_access_token(
            host=host, username=username, password=password
        )
        self.http_client = get_http_client(host=host, access_token=self.access_token)
        logger.info(f"Connected to {self.host} as {self.username}")

        self.default_region_name = default_region_name
        self.default_provider_name = (
            default_provider_name if default_provider_name else "AWS"
        )

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
        logger.info(f"Connection to {self.host} closed")
        global _firebolt_client_singleton
        _firebolt_client_singleton = None

    @classmethod
    def from_env(cls, dotenv_path: Optional[str] = None) -> FireboltClient:
        """
        Create a FireboltClient from the following environment variables:
        FIREBOLT_SERVER, FIREBOLT_USER, FIREBOLT_PASSWORD

        Load a .env file beforehand. Environment variables defined in .env will
        not overwrite values already present.

        Raise an exception if any of the environment variables are missing.

        Args:
            dotenv_path: path to a local .env file

        Returns:
            Initialized FireboltClient
        """
        # for local development: load any unset environment variables
        # that are defined in a `.env` file
        dotenv.load_dotenv(dotenv_path=dotenv_path, override=False)

        host = env.FIREBOLT_SERVER.get_required_value()
        username = env.FIREBOLT_USER.get_required_value()
        password = env.FIREBOLT_PASSWORD.get_required_value()
        default_region_name = env.FIREBOLT_DEFAULT_REGION.get_optional_value()
        default_provider_name = env.FIREBOLT_DEFAULT_PROVIDER.get_optional_value()

        return cls(
            host=host,
            username=username,
            password=password,
            default_region_name=default_region_name,
            default_provider_name=default_provider_name,
        )

    @cached_property
    def account_id(self) -> str:
        response = self.http_client.get(
            url="/iam/v2/account",
        )
        account_id = response.json()["account"]["id"]
        return account_id
