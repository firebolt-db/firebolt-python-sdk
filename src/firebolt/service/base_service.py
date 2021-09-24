from firebolt.client import FireboltClient


class BaseService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client
