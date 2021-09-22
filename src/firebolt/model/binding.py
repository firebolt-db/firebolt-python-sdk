from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, Field

from firebolt.common import prune_dict
from firebolt.model import FireboltBaseModel

if TYPE_CHECKING:
    from firebolt.model.database import Database
    from firebolt.model.engine import Engine


class BindingKey(BaseModel):
    account_id: str
    database_id: str
    engine_id: str


class Binding(FireboltBaseModel):
    """A Binding between an Engine and a Database"""

    binding_key: BindingKey = Field(alias="id")
    is_default_engine: bool = Field(alias="engine_is_default")

    # optional
    current_status: Optional[str]
    health_status: Optional[str]
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    desired_status: Optional[str]

    @classmethod
    def get_by_key(cls, binding_key: BindingKey) -> Binding:
        """Get a binding by it's BindingKey"""
        response = cls.get_firebolt_client().get(
            url=f"/core/v1/accounts/{binding_key.account_id}"
            f"/databases/{binding_key.database_id}"
            f"/bindings/{binding_key.engine_id}"
        )
        binding: dict = response.json()["binding"]
        return cls.parse_obj(binding)

    @classmethod
    def list_bindings(
        cls,
        database_id: Optional[str] = None,
        engine_id: Optional[str] = None,
        is_system_database: Optional[bool] = None,
    ) -> list[Binding]:
        """
        List bindings on Firebolt, optionally filtering by database and engine.

        Args:
            database_id:
                Return bindings matching the database_id.
                If None, match any databases.
            engine_id:
                Return bindings matching the engine_id.
                If None, match any engines.
            is_system_database:
                If True, return only system databases.
                If False, return only non-system databases.
                If None, do not filter on this parameter.

        Returns:
            List of bindings matching the filter parameters.
        """
        fc = cls.get_firebolt_client()
        response = fc.get(
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

    @classmethod
    def create(
        cls, engine: Engine, database: Database, is_default_engine: bool
    ) -> Binding:
        """
        Create a new binding between an engine and a database.

        Args:
            engine: Engine to bind.
            database: Database to bind.
            is_default_engine:
                Whether this engine should be used as default for this database.
                Only one engine can be set as default for a single database.
                This will overwrite any existing default.

        Returns:
            New binding between the engine and database.
        """
        firebolt_client = cls.get_firebolt_client()
        binding = cls(
            binding_key=BindingKey(
                account_id=firebolt_client.account_id,
                database_id=database.database_id,
                engine_id=engine.engine_id,
            ),
            is_default_engine=is_default_engine,
        )
        return binding.apply_create()

    @property
    def database_id(self) -> str:
        return self.binding_key.database_id

    @property
    def engine_id(self) -> str:
        return self.binding_key.engine_id

    def apply_create(self) -> Binding:
        """Send a request to create the Binding on Firebolt"""
        response = self.get_firebolt_client().post(
            url=f"/core/v1/accounts/{self.binding_key.account_id}"
            f"/databases/{self.database_id}"
            f"/bindings/{self.engine_id}",
            json=self.dict(
                by_alias=True, include={"binding_key": ..., "is_default_engine": ...}
            ),
        )
        return Binding.parse_obj(response.json()["binding"])
