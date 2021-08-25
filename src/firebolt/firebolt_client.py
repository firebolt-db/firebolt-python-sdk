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
        self, host: str, username: str, password: str, default_region_name: str
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
        # self._instance_types: Optional[list[InstanceType]] = None
        # self.databases = DatabaseService(firebolt_client=self)
        # self.engines = EngineService(firebolt_client=self)

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
        region_name = env.FIREBOLT_PROVIDER_REGION.get_value()

        return cls(
            host=host,
            username=username,
            password=password,
            default_region_name=region_name,
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

    def providers(self):
        response = self.http_client.get(
            url="/compute/v1/providers", params={"page.first": 5000}
        )
        # '402a51bb-1c8e-4dc4-9e05-ced3c1e2186e'
        # AWS
        return response.json()

    def regions(self):
        response = self.http_client.get(
            url="/compute/v1/regions", params={"page.first": 5000}
        )
        return response.json()

    # def get_instance_type_by_name(self, instance_name: str, region_name: Optional[str] =self.region_name):
    #     return self.get_instance_type_by_id(InstanceTypeId(
    #         provider_id=,
    #         region_id=,
    #         instance_type_id=,
    #     ))

    # def get_instance_type_by_id(self, instance_type_id: InstanceTypeId):
    #     return self.instance_types[instance_type_id]
    #
    # @property
    # def instance_types(self) -> list[InstanceType]:
    #     if not self._instance_types:
    #         response = self.http_client.get(
    #             url="/compute/v1/instanceTypes", params={"page.first": 5000}
    #         )
    #         self._instance_types =  [
    #             InstanceType.parse_obj(i["node"]) for i in response.json()["edges"]
    #         ]
    #         self._instance_types_by_region
    #     return self._instance_types


class FireboltClientMixin:
    @cached_property
    def firebolt_client(self) -> FireboltClient:
        return get_firebolt_client()
