import logging

import dotenv

from firebolt.common import env
from firebolt.http_client import get_http_client

logger = logging.getLogger(__name__)


class FireboltClient:
    def __init__(self, host: str, username: str, password: str):
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
        logger.info(f"Connected to {self.host} as {self.username}")

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

        return cls(host=host, username=username, password=password)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.http_client.close()
        logger.info(f"Connection to {self.host} closed")

    @property
    def account_id(self) -> str:
        response = self.http_client.get(
            url="/iam/v2/accounts:getIdByName",
            params={"account_name": self.account_name},
        )
        account_id = response.json()["account_id"]
        return account_id

    @property
    def databases(self):
        return DatabaseService(firebolt_client=self)

    @property
    def engines(self):
        return EngineService(firebolt_client=self)


class DatabaseService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client
        self.http_client = firebolt_client.http_client

    def get_id_by_name(self, database_name: str) -> str:
        response = self.http_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def get_by_id(self, database_id: str):
        response = self.http_client.get(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/databases/{database_id}",
        )
        spec = response.json()["database"]
        return spec

    def get_by_name(self, database_name: str):
        database_id = self.get_id_by_name(database_name=database_name)
        return self.get_by_id(database_id=database_id)


class EngineService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client
        self.http_client = firebolt_client.http_client

    def get_engine_id_by_name(self, engine_name: str) -> str:
        response = self.http_client.get(
            url=f"core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return engine_id

    def start_engine(self, engine_id: str) -> str:
        response = self.http_client.get(
            url=f"core/v1/account/engines/{engine_id}:start",
        )
        status = response.json()["engine"]["current_status_summary"]
        return status
