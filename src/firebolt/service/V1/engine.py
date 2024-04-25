from logging import getLogger
from typing import List, Optional, Union

from firebolt.model.V1.engine import Engine
from firebolt.service.V1.base import BaseService
from firebolt.service.V1.types import EngineOrder
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
                    "page.first": 5000,
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
