from firebolt.client import Client
from firebolt.common import Settings
from firebolt.service.manager import ResourceManager


class BaseService:
    def __init__(self, resource_manager: ResourceManager):
        self.resource_manager = resource_manager

    @property
    def client(self) -> Client:
        return self.resource_manager.client

    @property
    def account_id(self) -> str:
        return self.resource_manager.account_id

    @property
    def settings(self) -> Settings:
        return self.resource_manager.settings
