from firebolt.client import FireboltResourceClient


class BaseService:
    def __init__(self, firebolt_client: FireboltResourceClient):
        self.firebolt_client = firebolt_client

    @property
    def account_id(self) -> str:
        return self.firebolt_client.account_id
