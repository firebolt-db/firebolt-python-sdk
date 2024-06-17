from typing import TYPE_CHECKING

from firebolt.db import Connection

if TYPE_CHECKING:
    from firebolt.service.manager import ResourceManager


class BaseService:
    def __init__(self, resource_manager: "ResourceManager"):
        self.resource_manager = resource_manager

    @property
    def _connection(self) -> Connection:
        return self.resource_manager._connection
