from typing import Optional

from firebolt.client import Client, log_request, log_response, raise_on_4xx_5xx
from firebolt.common import Settings
from firebolt.service.provider import get_provider_id


class ResourceManager:
    """
    ResourceManager to access various Firebolt resources:

    - databases
    - engines
    - bindings (the bindings between an engine and a database)
    - engine revisions (versions of an engine)

    Also provides listings of:
    - regions (AWS regions in which engines can run)
    - instance types (AWS instance types which engines can use)
    """

    def __init__(self, settings: Optional[Settings] = None):
        if settings is None:
            settings = Settings()
        self.settings = settings

        self.client = Client(
            auth=(self.settings.user, self.settings.password.get_secret_value()),
            base_url=f"https://{ self.settings.server}",
            api_endpoint=self.settings.server,
        )
        self.client.event_hooks = {
            "request": [log_request],
            "response": [raise_on_4xx_5xx, log_response],
        }
        self._init_services()

    def _init_services(self) -> None:
        # avoid circular import
        from firebolt.service.binding import BindingService
        from firebolt.service.database import DatabaseService
        from firebolt.service.engine import EngineService
        from firebolt.service.engine_revision import EngineRevisionService
        from firebolt.service.instance_type import InstanceTypeService
        from firebolt.service.region import RegionService

        # Cloud Platform Resources (AWS)
        self.regions = RegionService(resource_manager=self)
        self.instance_types = InstanceTypeService(resource_manager=self)
        self.provider_id = get_provider_id(client=self.client)

        # Firebolt Resources
        self.databases = DatabaseService(resource_manager=self)
        self.engines = EngineService(resource_manager=self)
        self.engine_revisions = EngineRevisionService(resource_manager=self)
        self.bindings = BindingService(resource_manager=self)
