from typing import TYPE_CHECKING, Optional

from firebolt.client import ClientV1 as Client

if TYPE_CHECKING:
    from firebolt.service.manager import ResourceManager


class BaseService:
    def __init__(self, resource_manager: "ResourceManager"):
        self.resource_manager = resource_manager

    @property
    def client(self) -> Client:
        return self.resource_manager._client

    @property
    def account_id(self) -> str:
        return self.resource_manager.account_id

    @property
    def default_region_setting(self) -> Optional[str]:
        return self.resource_manager.default_region
