import logging
from typing import List, Optional

from firebolt.model.V1.binding import Binding
from firebolt.service.V1.base import BaseService
from firebolt.utils.urls import ACCOUNT_BINDINGS_URL
from firebolt.utils.util import prune_dict

logger = logging.getLogger(__name__)


class BindingService(BaseService):
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
        return [Binding.parse_obj(i["node"]) for i in response.json()["edges"]]
