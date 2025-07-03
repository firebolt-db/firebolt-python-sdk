import logging
from typing import Optional

from httpx import Timeout

from firebolt.client import (
    DEFAULT_API_URL,
    ClientV1,
    ClientV2,
    log_request,
    log_response,
    raise_on_4xx_5xx,
)
from firebolt.client.auth import Auth
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.common import Settings
from firebolt.db import connect
from firebolt.service.V1.binding import BindingService
from firebolt.service.V1.database import DatabaseService as DatabaseServiceV1
from firebolt.service.V1.engine import EngineService as EngineServiceV1
from firebolt.service.V1.provider import get_provider_id
from firebolt.service.V1.region import RegionService
from firebolt.service.V2.database import DatabaseService as DatabaseServiceV2
from firebolt.service.V2.engine import EngineService as EngineServiceV2
from firebolt.service.V2.instance_type import InstanceTypeService
from firebolt.utils.util import fix_url_schema

DEFAULT_TIMEOUT_SECONDS: int = 60 * 2

logger = logging.getLogger(__name__)

SETTINGS_DEPRECATION_MESSAGE = """
Using Settings objects for ResourceManager intialization is deprecated.
Please pass parameters directly
Example:
 >>> rm = ResourceManager(auth=ClientCredentials(..), account_name="my_account", ..)
"""


class ResourceManager:
    """
    ResourceManager to access various Firebolt resources:

    - databases
    - engines

    Also provides listings of:

    - instance types (AWS instance types which engines can use)
    """

    __slots__ = (
        "account_name",
        "account_id",
        "api_endpoint",
        "_client",
        "_connection",
        "regions",
        "instance_types",
        "provider_id",
        "databases",
        "engines",
        "engine_revisions",
        "bindings",
        "default_region",
        "_version",
    )

    def __init__(
        self,
        settings: Optional[Settings] = None,
        auth: Optional[Auth] = None,
        account_name: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
        # Legacy parameters
        default_region: Optional[str] = None,
    ):
        if settings:
            logger.warning(SETTINGS_DEPRECATION_MESSAGE)
            if auth or account_name or (api_endpoint != DEFAULT_API_URL):
                raise ValueError(
                    "Other ResourceManager parameters are not allowed "
                    "when Settings are provided"
                )
            auth = settings.auth
            account_name = settings.account_name
            api_endpoint = settings.server
            default_region = settings.default_region

        for param, name in ((auth, "auth"),):
            if not param:
                raise ValueError(f"Missing {name} value")

        # type checks
        assert auth is not None

        version = auth.get_firebolt_version()
        if version == FireboltAuthVersion.V2:
            client_class = ClientV2
            assert account_name is not None
        elif version == FireboltAuthVersion.V1:
            client_class = ClientV1
        else:
            raise ValueError(f"Unsupported Firebolt version: {version}")
        # Separate client for API calls, not via engine URL
        self._client = client_class(
            auth=auth,
            base_url=fix_url_schema(api_endpoint),
            account_name=account_name,  # type: ignore # already checked above for v2
            api_endpoint=api_endpoint,
            timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
            event_hooks={
                "request": [log_request],
                "response": [raise_on_4xx_5xx, log_response],
            },
        )
        # V1 does not use a DB connection
        if version != 1:
            self._connection = connect(
                auth=auth,
                account_name=account_name,
                api_endpoint=api_endpoint,
            )
        else:
            self._connection = None  # type: ignore
        self.account_name = account_name
        self.api_endpoint = api_endpoint
        self.account_id = self._client.account_id
        self.default_region = default_region
        self.provider_id: Optional[str] = None
        if version == 2:
            self._init_services_v2()
        elif version == 1:
            self._init_services_v1()

    def _init_services_v2(self) -> None:
        # avoid circular import

        # Cloud Platform Resources (AWS)
        self.instance_types = InstanceTypeService(resource_manager=self)

        # Firebolt Resources
        self.databases = DatabaseServiceV2(resource_manager=self)
        self.engines = EngineServiceV2(resource_manager=self)

        # Not applicable to V2
        self.provider_id = None

    def _init_services_v1(self) -> None:
        # Cloud Platform Resources (AWS)
        self.regions = RegionService(resource_manager=self)  # type: ignore

        # Firebolt Resources
        self.bindings = BindingService(resource_manager=self)  # type: ignore
        self.engines = EngineServiceV1(resource_manager=self)  # type: ignore
        self.databases = DatabaseServiceV1(resource_manager=self)  # type: ignore

        self.provider_id = get_provider_id(client=self._client)

    def __del__(self) -> None:
        if hasattr(self, "_client"):
            self._client.close()
        if hasattr(self, "_connection") and self._connection is not None:
            self._connection.close()
