from __future__ import annotations

from typing import TYPE_CHECKING

from firebolt.model.engine import Engine
from firebolt.model.engine_revision import EngineRevision

if TYPE_CHECKING:  # prevent circular imports
    from firebolt.firebolt_client import FireboltClient


class EngineService:
    def __init__(self, firebolt_client: FireboltClient):
        self.firebolt_client = firebolt_client
        self.http_client = firebolt_client.http_client

    def get_by_id(self, engine_id: str) -> Engine:
        response = self.http_client.get(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/engines/{engine_id}",
        )
        engine_spec: dict = response.json()["engine"]
        return Engine.parse_obj(engine_spec)

    def get_by_name(self, engine_name: str) -> Engine:
        engine_id = self.get_engine_id_by_name(engine_name=engine_name)
        return self.get_by_id(engine_id=engine_id)

    def get_engine_id_by_name(self, engine_name: str) -> str:
        response = self.http_client.get(
            url=f"/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return engine_id

    def start_engine_by_id(self, engine_id: str) -> str:
        response = self.http_client.post(
            url=f"/core/v1/account/engines/{engine_id}:start",
        )
        status = response.json()["engine"]["current_status_summary"]
        return status

    def start_engine_by_name(self, engine_name: str) -> str:
        engine_id = self.get_engine_id_by_name(engine_name=engine_name)
        return self.start_engine_by_id(engine_id=engine_id)

    def create_engine(self, engine: Engine):
        response = self.http_client.post(
            url=f"/core/v1/account/engines", json=engine.json()
        )

    def get_engine_revision_by_id(
        self, engine_id: str, engine_revision_id: str
    ) -> EngineRevision:
        response = self.http_client.get(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/engines/{engine_id}/engineRevisions/{engine_revision_id}",
        )
        engine_revision_spec: dict = response.json()["engine_revision"]
        return EngineRevision.parse_obj(engine_revision_spec)
