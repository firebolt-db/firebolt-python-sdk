from firebolt.client import FireboltClient


class BaseService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client

    @property
    def account_id(self) -> str:
        return self.firebolt_client.account_id
