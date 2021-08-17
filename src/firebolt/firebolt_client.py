import dotenv

from firebolt.common import env
from firebolt.http_client import get_http_client


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

    @classmethod
    def from_env(cls, dotenv_path=None):
        # for development: load any unset environment variables that are defined in a `.env` file
        dotenv.load_dotenv(dotenv_path=dotenv_path, override=False)

        host = env.FIREBOLT_SERVER.get_value()
        username = env.FIREBOLT_USER.get_value()
        password = env.FIREBOLT_PASSWORD.get_value()

        return cls(host=host, username=username, password=password)

    @property
    def account_id(self) -> str:
        response = self.http_client.get(
            url="/iam/v2/accounts:getIdByName",
            params={"account_name": self.account_name},
        )
        account_id = response.json()["account_id"]
        return account_id

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


firebolt_client = FireboltClient.from_env()  # "singleton"


# client = FireboltClient.from_env()
# print(client.account_id())
# print(client.database_id(database_name='eg_sandbox'))
# print(client.get_engine_id_by_name("eg_sandbox_analytics"))
