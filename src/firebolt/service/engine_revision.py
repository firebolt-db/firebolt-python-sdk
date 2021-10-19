from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey
from firebolt.service.base import BaseService


class EngineRevisionService(BaseService):
    def get_by_id(self, engine_id: str, engine_revision_id: str) -> EngineRevision:
        """Get an EngineRevision from Firebolt by engine_id and engine_revision_id."""
        return self.get_by_key(
            EngineRevisionKey(
                account_id=self.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    def get_by_key(self, key: EngineRevisionKey) -> EngineRevision:
        """
        Fetch an EngineRevision from Firebolt by it's key.

        Args:
            key: Key of the desired EngineRevision.

        Returns:
            The requested EngineRevision
        """
        response = self.client.get(
            url=f"/core/v1/accounts/{key.account_id}"
            f"/engines/{key.engine_id}"
            f"/engineRevisions/{key.engine_revision_id}",
        )
        engine_spec: dict = response.json()["engine_revision"]
        return EngineRevision.parse_obj(engine_spec)
