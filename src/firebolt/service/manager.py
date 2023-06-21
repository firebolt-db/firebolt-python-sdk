import logging
from typing import Optional

from httpx import Timeout

from firebolt.client import (
    DEFAULT_API_URL,
    Auth,
    Client,
    log_request,
    log_response,
    raise_on_4xx_5xx,
)
from firebolt.common import Settings
from firebolt.db import connect
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
        "_provider_id",
        "databases",
        "engines",
        "engine_revisions",
        "bindings",
    )

    def __init__(
        self,
        settings: Optional[Settings] = None,
        auth: Optional[Auth] = None,
        account_name: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
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

        for param, name in (
            (auth, "auth"),
            (account_name, "account_name"),
        ):
            if not param:
                raise ValueError(f"Missing {name} value")

        # type checks
        assert auth is not None
        assert account_name is not None

        self._client = Client(
            auth=auth,
            base_url=fix_url_schema(api_endpoint),
            account_name=account_name,
            api_endpoint=api_endpoint,
            timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
            event_hooks={
                "request": [log_request],
                "response": [raise_on_4xx_5xx, log_response],
            },
        )
        self._connection = connect(
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
        )
        self.account_name = account_name
        self.api_endpoint = api_endpoint
        self.account_id = self._client.account_id
        self._init_services()

    def _init_services(self) -> None:
        # avoid circular import
        from firebolt.service.database import DatabaseService
        from firebolt.service.engine import EngineService
        from firebolt.service.instance_type import InstanceTypeService

        # Cloud Platform Resources (AWS)
        self.instance_types = InstanceTypeService(resource_manager=self)

        # Firebolt Resources
        self.databases = DatabaseService(resource_manager=self)
        self.engines = EngineService(resource_manager=self)

    def __del__(self) -> None:
        if hasattr(self, "_client"):
            self._client.close()
        if hasattr(self, "_connection"):
            self._connection.close()
