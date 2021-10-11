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

        self.client = Client(
            auth=(settings.user, settings.password.get_secret_value()),
            base_url=f"https://{settings.server}",
            api_endpoint=settings.server,
        )
        self.client.event_hooks = {
            "request": [log_request],
            "response": [log_response, raise_on_4xx_5xx],
        }
        self._init_services(default_region_name=settings.default_region)

    def _init_services(self, default_region_name: str) -> None:
        # avoid circular import
        from firebolt.service.binding import BindingService
        from firebolt.service.database import DatabaseService
        from firebolt.service.engine import EngineService
        from firebolt.service.engine_revision import EngineRevisionService
        from firebolt.service.instance_type import InstanceTypeService
        from firebolt.service.region import RegionService

        # Cloud Platform Resources (AWS, etc)
        self.regions = RegionService(
            resource_manager=self, default_region_name=default_region_name
        )
        self.instance_types = InstanceTypeService(resource_manager=self)
        self.provider_id = get_provider_id(client=self.client)

        # Firebolt Resources
        self.databases = DatabaseService(resource_manager=self)
        self.engines = EngineService(resource_manager=self)
        self.engine_revisions = EngineRevisionService(resource_manager=self)
        self.bindings = BindingService(resource_manager=self)
