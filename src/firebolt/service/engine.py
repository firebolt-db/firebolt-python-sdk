from logging import getLogger
from typing import Any, Dict, List, Optional, Union

from firebolt.model.engine import Engine, EngineSettings, _EngineCreateRequest
from firebolt.model.engine_revision import (
    EngineRevision,
    EngineRevisionSpecification,
)
from firebolt.model.region import Region
from firebolt.service.base import BaseService
from firebolt.service.types import EngineOrder, EngineType, WarmupMethod
from firebolt.utils.exception import FireboltError
from firebolt.utils.urls import (
    ACCOUNT_ENGINE_ID_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_LIST_ENGINES_URL,
    ENGINES_BY_IDS_URL,
)
from firebolt.utils.util import prune_dict

logger = getLogger(__name__)


class EngineService(BaseService):
    def get(self, id_: str) -> Engine:
        """Get an engine from Firebolt by its ID."""

        response = self.client.get(
            url=ACCOUNT_ENGINE_URL.format(account_id=self.account_id, engine_id=id_),
        )
        engine_entry: dict = response.json()["engine"]
        return Engine.parse_obj_with_service(obj=engine_entry, engine_service=self)

    def get_by_ids(self, ids: List[str]) -> List[Engine]:
        """Get multiple engines from Firebolt by ID."""
        response = self.client.post(
            url=ENGINES_BY_IDS_URL,
            json={
                "engine_ids": [
                    {"account_id": self.account_id, "engine_id": engine_id}
                    for engine_id in ids
                ]
            },
        )
        return [
            Engine.parse_obj_with_service(obj=e, engine_service=self)
            for e in response.json()["engines"]
        ]

    def get_by_name(self, name: str) -> Engine:
        """Get an engine from Firebolt by its name."""

        response = self.client.get(
            url=ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=self.account_id),
            params={"engine_name": name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return self.get(id_=engine_id)

    def get_many(
        self,
        name_contains: Optional[str] = None,
        current_status_eq: Optional[str] = None,
        current_status_not_eq: Optional[str] = None,
        region_eq: Optional[str] = None,
        order_by: Optional[Union[str, EngineOrder]] = None,
    ) -> List[Engine]:
        """
        Get a list of engines on Firebolt.

        Args:
            name_contains: Filter for engines with a name containing this substring
            current_status_eq: Filter for engines with this status
            current_status_not_eq: Filter for engines that do not have this status
            region_eq: Filter for engines by region
            order_by: Method by which to order the results. See [EngineOrder]

        Returns:
            A list of engines matching the filters
        """

        if isinstance(order_by, str):
            order_by = EngineOrder[order_by].name

        if region_eq is not None:
            region_eq = self.resource_manager.regions.get_by_name(
                name=region_eq
            ).key.region_id

        response = self.client.get(
            url=ACCOUNT_LIST_ENGINES_URL.format(account_id=self.account_id),
            params=prune_dict(
                {
                    "page.first": 5000,  # FUTURE: pagination support w/ generator
                    "filter.name_contains": name_contains,
                    "filter.current_status_eq": current_status_eq,
                    "filter.current_status_not_eq": current_status_not_eq,
                    "filter.compute_region_id_region_id_eq": region_eq,
                    "order_by": order_by,
                }
            ),
        )
        return [
            Engine.parse_obj_with_service(obj=e["node"], engine_service=self)
            for e in response.json()["edges"]
        ]

    def create(
        self,
        name: str,
        region: Union[str, Region, None] = None,
        engine_type: Union[str, EngineType] = EngineType.GENERAL_PURPOSE,
        scale: int = 2,
        spec: Optional[str] = None,
        auto_stop: int = 20,
        warmup: Union[str, WarmupMethod] = WarmupMethod.PRELOAD_INDEXES,
        description: str = "",
        engine_settings_kwargs: Dict[str, Any] = {},
        revision_spec_kwargs: Dict[str, Any] = {},
    ) -> Engine:
        """
        Create a new engine.

        Args:
            name: An identifier that specifies the name of the engine
            region: The AWS region in which the engine runs
            engine_type: The engine type. GENERAL_PURPOSE or DATA_ANALYTICS
            scale: The number of compute instances on the engine.
                The scale can be any int from 1 to 128.
            spec: Firebolt instance type. If not set, will default to
                the cheapest instance.
            auto_stop: The amount of time (in minutes)
            after which the engine automatically stops
            warmup: The warmup method that should be used:

                `MINIMAL` - On-demand loading (both indexes and tables' data)

                `PRELOAD_INDEXES` - Load indexes only

                `PRELOAD_ALL_DATA` - Full data auto-load
                (both indexes and table data - full warmup)
            description: A short description of the engine's purpose

        Returns:
            Engine with the specified settings
        """

        logger.info(f"Creating Engine (name={name})")

        if isinstance(engine_type, str):
            engine_type = EngineType[engine_type]
        if isinstance(warmup, str):
            warmup = WarmupMethod[warmup]

        if region is None:
            region = self.resource_manager.regions.default_region
        else:
            if isinstance(region, str):
                region = self.resource_manager.regions.get_by_name(name=region)

        engine = Engine(
            name=name,
            description=description,
            compute_region_key=region.key,
            settings=EngineSettings.default(
                engine_type=engine_type,
                auto_stop_delay_duration=f"{auto_stop * 60}s",
                warm_up=warmup,
                **engine_settings_kwargs,
            ),
        )

        if spec:
            instance_type_key = self.resource_manager.instance_types.get_by_name(
                instance_type_name=spec, region_name=region.name
            ).key
        else:
            instance_type = (
                self.resource_manager.instance_types.cheapest_instance_in_region(region)
            )
            if not instance_type:
                raise FireboltError(
                    f"No suitable default instances found in region {region}"
                )
            instance_type_key = instance_type.key

        engine_revision = EngineRevision(
            specification=EngineRevisionSpecification(
                db_compute_instances_type_key=instance_type_key,
                db_compute_instances_count=scale,
                proxy_instances_type_key=instance_type_key,
                **revision_spec_kwargs,
            )
        )

        return self._send_create_engine(engine=engine, engine_revision=engine_revision)

    def _send_create_engine(
        self, engine: Engine, engine_revision: Optional[EngineRevision] = None
    ) -> Engine:
        """
        Create a new Engine on Firebolt from the local Engine object.

        Args:
            engine: The engine to create
            engine_revision: EngineRevision to use for configuring the engine

        Returns:
            The newly created engine
        """

        response = self.client.post(
            url=ACCOUNT_LIST_ENGINES_URL.format(account_id=self.account_id),
            headers={"Content-type": "application/json"},
            json=_EngineCreateRequest(
                account_id=self.account_id,
                engine=engine,
                engine_revision=engine_revision,
            ).jsonable_dict(by_alias=True),
        )
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self
        )
