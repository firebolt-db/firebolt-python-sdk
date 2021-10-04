from typing import Optional

from firebolt.common import AlreadyBoundError, prune_dict
from firebolt.model.binding import Binding, BindingKey
from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.service.base import BaseService


class BindingService(BaseService):
    def get_by_key(self, binding_key: BindingKey) -> Binding:
        """Get a binding by it's BindingKey"""
        response = self.client.get(
            url=f"/core/v1/accounts/{binding_key.account_id}"
            f"/databases/{binding_key.database_id}"
            f"/bindings/{binding_key.engine_id}"
        )
        binding: dict = response.json()["binding"]
        return Binding.parse_obj(binding)

    def list_bindings(
        self,
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
        response = self.client.get(
            url=f"/core/v1/accounts/{self.account_id}/bindings",
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

    def get_database_bound_to_engine(self, engine: Engine) -> Optional[Database]:
        """Get the Database to which an engine is bound, if any."""
        try:
            binding = self.list_bindings(engine_id=engine.engine_id)[0]
            return self.resource_manager.databases.get_by_id(
                database_id=binding.database_id
            )
        except IndexError:
            return None

    def get_engines_bound_to_database(self, database: Database) -> list[Engine]:
        """Get a list of engines that are bound to a database."""
        bindings = self.list_bindings(database_id=database.database_id)
        return self.resource_manager.engines.get_engines_by_ids(
            engine_ids=[b.engine_id for b in bindings]
        )

    def create_binding(
        self, engine: Engine, database: Database, is_default_engine: bool
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
        existing_database = self.get_database_bound_to_engine(engine=engine)
        if existing_database is not None:
            raise AlreadyBoundError(
                f"The engine {engine.name} is already bound "
                f"to {existing_database.name}!"
            )

        binding = Binding(
            binding_key=BindingKey(
                account_id=self.account_id,
                database_id=database.database_id,
                engine_id=engine.engine_id,
            ),
            is_default_engine=is_default_engine,
        )

        response = self.client.post(
            url=f"/core/v1/accounts/{self.account_id}"
            f"/databases/{database.database_id}"
            f"/bindings/{engine.engine_id}",
            json=binding.dict(
                by_alias=True, include={"binding_key": ..., "is_default_engine": ...}
            ),
        )
        return Binding.parse_obj(response.json()["binding"])
