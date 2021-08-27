from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from firebolt.common.data import prune_dict
from firebolt.firebolt_client import FireboltClientMixin


class BindingKey(BaseModel):
    account_id: str
    database_id: str
    engine_id: str


class Binding(BaseModel, FireboltClientMixin):
    binding_key: BindingKey = Field(alias="id")
    engine_is_default: bool
    current_status: str
    health_status: str
    create_time: datetime
    create_actor: str
    last_update_time: datetime
    last_update_actor: str
    desired_status: str

    @property
    def database_id(self) -> str:
        return self.binding_key.database_id

    @classmethod
    def get_by_key(cls, binding_key: BindingKey) -> Binding:
        response = cls.get_firebolt_client().http_client.get(
            url=f"/core/v1/accounts/{binding_key.account_id}"
            f"/databases/{binding_key.database_id}"
            f"/bindings/{binding_key.engine_id}"
        )
        binding: dict = response.json()["binding"]
        return cls.parse_obj(binding)

    def create(self):
        response = self.firebolt_client.http_client.post(
            url=f"/core/v1/accounts/{self.binding_key.account_id}"
            f"/databases/{self.binding_key.database_id}"
            f"/bindings/{self.binding_key.engine_id}",
            json=self.json(
                by_alias=True, include={"id": ..., "engine_is_default": ...}
            ),
        )
        return response.json()

    @classmethod
    def list(
        cls,
        database_id: Optional[str] = None,
        engine_id: Optional[str] = None,
        is_system_database: Optional[bool] = None,
    ) -> list[Binding]:
        fc = cls.get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/bindings",
            params=prune_dict(
                {
                    "page.first": 5000,  # FUTURE: consider changing this to a generator
                    "filter.id_database_id_eq": database_id,
                    "filter.id_engine_id_eq": engine_id,
                    "filter.is_system_database_eq": is_system_database,
                }
            ),
        )
        return [Binding.parse_obj(i["node"]) for i in response.json()["edges"]]
