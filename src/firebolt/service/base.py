from firebolt.client import Client
from firebolt.service.manager import ResourceManager


class BaseService:
    def __init__(self, resource_manager: ResourceManager):
        self.resource_manager = resource_manager

    @property
    def client(self) -> Client:
        return self.resource_manager.client

    @property
    def account_id(self) -> str:
        return self.client.account_id
