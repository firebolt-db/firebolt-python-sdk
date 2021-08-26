# from firebolt.api.database_service import DatabaseService
# from firebolt.api.engine_service import EngineService
from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

import dotenv

from firebolt.common import env
from firebolt.common.exception import FireboltClientRequiredError
from firebolt.http_client import get_http_client

logger = logging.getLogger(__name__)

_firebolt_client_singleton: Optional[FireboltClient] = None


def get_firebolt_client() -> FireboltClient:
    global _firebolt_client_singleton
    if _firebolt_client_singleton is None:
        raise FireboltClientRequiredError()
    return _firebolt_client_singleton


class FireboltClient:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        default_region_name: Optional[str] = None,
        default_provider_name: Optional[str] = None,
    ):
        self.username = username
        try:
            self.account_name = username.split("@")[1].split(".")[0]
        except IndexError:
            raise ValueError(
                "Invalid username. Your username should be a valid email address, including a domain."
            )
        self.password = password
        self.host = host
        self.http_client = get_http_client(
            host=host, username=username, password=password
        )
        logger.info(
            f"Connected to {self.host} as {self.username} (account_id:{self.account_id})"
        )

        self.default_region_name = default_region_name
        self.default_provider_name = (
            default_provider_name if default_provider_name else "AWS"
        )

    @classmethod
    def from_env(cls, dotenv_path=None):
        """
        Create a FireboltClient from the following environment variables:
        FIREBOLT_SERVER, FIREBOLT_USER, FIREBOLT_PASSWORD

        Load a .env file beforehand. Environment variables defined in .env will not overwrite values already present.

        Raise an exception if any of the environment variables are missing.

        :param dotenv_path: (Optional) path to a local .env file
        :return: Initialized FireboltClient
        """
        # for local development: load any unset environment variables that are defined in a `.env` file
        dotenv.load_dotenv(dotenv_path=dotenv_path, override=False)

        host = env.FIREBOLT_SERVER.get_value()
        username = env.FIREBOLT_USER.get_value()
        password = env.FIREBOLT_PASSWORD.get_value()
        default_region_name = env.FIREBOLT_DEFAULT_REGION.get_value(is_required=False)
        default_provider_name = env.FIREBOLT_DEFAULT_PROVIDER.get_value(
            is_required=False
        )

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
            url="/iam/v2/accounts:getIdByName",
            params={"account_name": self.account_name},
        )
        account_id = response.json()["account_id"]
        return account_id

    def __enter__(self):
        global _firebolt_client_singleton
        _firebolt_client_singleton = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.http_client.close()
        logger.info(f"Connection to {self.host} closed")
        global _firebolt_client_singleton
        _firebolt_client_singleton = None


class FireboltClientMixin:
    @cached_property
    def firebolt_client(self) -> FireboltClient:
        return get_firebolt_client()
