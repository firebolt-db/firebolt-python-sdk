import logging
from typing import List, Optional

from firebolt.model.V1.binding import Binding, BindingKey
from firebolt.model.V1.database import Database
from firebolt.model.V1.engine import Engine
from firebolt.service.V1.base import BaseService
from firebolt.service.V1.database import DatabaseService
from firebolt.service.V1.engine import EngineService
from firebolt.utils.exception import AlreadyBoundError
from firebolt.utils.urls import (
    ACCOUNT_BINDINGS_URL,
    ACCOUNT_DATABASE_BINDING_URL,
)
from firebolt.utils.util import prune_dict

logger = logging.getLogger(__name__)


class BindingService(BaseService):
    def get_by_key(self, binding_key: BindingKey) -> Binding:
        """Get a binding by its BindingKey"""
        response = self.client.get(
            url=ACCOUNT_DATABASE_BINDING_URL.format(
                account_id=binding_key.account_id,
                database_id=binding_key.database_id,
                engine_id=binding_key.engine_id,
            )
        )
        binding: dict = response.json()["binding"]
        return Binding.parse_model(binding)

    def get_many(
        self,
        database_id: Optional[str] = None,
        engine_id: Optional[str] = None,
        is_system_database: Optional[bool] = None,
    ) -> List[Binding]:
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
            List of bindings matching the filter parameters
        """

        response = self.client.get(
            url=ACCOUNT_BINDINGS_URL.format(account_id=self.account_id),
            params=prune_dict(
                {
                    "page.first": 5000,  # FUTURE: pagination support w/ generator
                    "filter.id_database_id_eq": database_id,
                    "filter.id_engine_id_eq": engine_id,
                    "filter.is_system_database_eq": is_system_database,
                }
            ),
        )
        return [Binding.parse_model(i["node"]) for i in response.json()["edges"]]

    def get_database_bound_to_engine(self, engine: Engine) -> Optional[Database]:
        """Get the database to which an engine is bound, if any."""
        try:
            binding = self.get_many(engine_id=engine.engine_id)[0]
        except IndexError:
            return None
        try:
            assert isinstance(
                self.resource_manager.databases, DatabaseService
            ), "Expected DatabaseService V1"
            return self.resource_manager.databases.get(id_=binding.database_id)
        except (KeyError, IndexError):
            return None

    def get_engines_bound_to_database(self, database: Database) -> List[Engine]:
        """Get a list of engines that are bound to a database."""

        bindings = self.get_many(database_id=database.database_id)
        if not bindings:
            return []
        assert isinstance(
            self.resource_manager.engines, EngineService
        ), "Expected EngineService V1"
        return self.resource_manager.engines.get_by_ids(
            ids=[b.engine_id for b in bindings]
        )

    def create(
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

        logger.info(
            f"Attaching Engine (engine_id={engine.engine_id}, name={engine.name}) "
            f"to Database (database_id={database.database_id}, "
            f"name={database.name})"
        )
        assert database.database_id is not None, "Database must have database_id"
        binding = Binding(
            binding_key=BindingKey(  # type: ignore[call-arg]
                account_id=self.account_id,
                database_id=database.database_id,
                engine_id=engine.engine_id,
            ),
            is_default_engine=is_default_engine,  # type: ignore[call-arg]
        )

        response = self.client.post(
            url=ACCOUNT_DATABASE_BINDING_URL.format(
                account_id=self.account_id,
                database_id=database.database_id,
                engine_id=engine.engine_id,
            ),
            json=binding.jsonable_dict(
                by_alias=True, include={"binding_key": ..., "is_default_engine": ...}
            ),
        )
        return Binding.parse_model(response.json()["binding"])
