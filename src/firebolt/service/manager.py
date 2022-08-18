from typing import Optional

from httpx import Timeout

from firebolt.client import Client, log_request, log_response, raise_on_4xx_5xx
from firebolt.client.auth import Token, UsernamePassword
from firebolt.common import Settings
from firebolt.service.provider import get_provider_id
from firebolt.utils.util import fix_url_schema

DEFAULT_TIMEOUT_SECONDS: int = 60 * 2


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
        self.settings = settings or Settings()

        auth = self.settings.auth

        # Deprecated: we shouldn't support passing credentials after 1.0 release
        if auth is None:
            if self.settings.access_token:
                auth = Token(self.settings.access_token)
            else:
                auth = UsernamePassword(
                    self.settings.user,
                    self.settings.password.get_secret_value(),
                    self.settings.use_token_cache,
                )

        self.client = Client(
            auth=auth,
            base_url=fix_url_schema(self.settings.server),
            account_name=self.settings.account_name,
            api_endpoint=self.settings.server,
            timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
            event_hooks={
                "request": [log_request],
                "response": [raise_on_4xx_5xx, log_response],
            },
        )
        self.account_id = self.client.account_id
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
