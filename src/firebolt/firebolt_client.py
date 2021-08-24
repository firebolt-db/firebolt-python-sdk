import logging

import dotenv

# from firebolt.api.database_service import DatabaseService
# from firebolt.api.engine_service import EngineService
from firebolt.common import env
from firebolt.common.exception import FireboltClientRequiredError
from firebolt.http_client import get_http_client

logger = logging.getLogger(__name__)

_firebolt_client_singleton = None


def get_firebolt_client():
    global _firebolt_client_singleton
    if _firebolt_client_singleton is None:
        raise FireboltClientRequiredError()
    return _firebolt_client_singleton


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
        self.account_id = self._get_account_id()
        logger.info(
            f"Connected to {self.host} as {self.username} (account_id:{self.account_id})"
        )

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

        return cls(host=host, username=username, password=password)

    def _get_account_id(self) -> str:
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
