from typing import Optional

from firebolt.client import (
    FireboltClient,
    log_request,
    log_response,
    raise_on_4xx_5xx,
)
from firebolt.common import Settings


class ResourceManager:
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()

        self.client = FireboltClient(
            auth=(self.settings.user, self.settings.password.get_secret_value()),
            base_url=f"https://{self.settings.server}",
            api_endpoint=self.settings.server,
        )
        self.client.event_hooks = {
            "request": [log_request],
            "response": [log_response, raise_on_4xx_5xx],
        }
        self._init_services()

    def _init_services(self) -> None:
        # avoid circular import
        from firebolt.service.binding import BindingService
        from firebolt.service.database import DatabaseService
        from firebolt.service.engine import EngineService
        from firebolt.service.engine_revision import EngineRevisionService
        from firebolt.service.instance_type import InstanceTypeService
        from firebolt.service.provider import ProviderService
        from firebolt.service.region import RegionService

        # Cloud Platform Resources (AWS, etc)
        self.providers = ProviderService(resource_manager=self)
        self.regions = RegionService(resource_manager=self)
        self.instance_types = InstanceTypeService(resource_manager=self)

        # Firebolt Resources
        self.databases = DatabaseService(resource_manager=self)
        self.engines = EngineService(resource_manager=self)
        self.engine_revisions = EngineRevisionService(resource_manager=self)
        self.bindings = BindingService(resource_manager=self)
