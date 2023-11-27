from logging import getLogger
from typing import List

from firebolt.model.V1.engine import Engine
from firebolt.service.V1.base import BaseService
from firebolt.utils.urls import (
    ACCOUNT_ENGINE_ID_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ENGINES_BY_IDS_URL,
)

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
