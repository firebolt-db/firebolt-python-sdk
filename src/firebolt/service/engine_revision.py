from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey
from firebolt.service.base import BaseService
from firebolt.utils.urls import ACCOUNT_ENGINE_REVISION_URL


class EngineRevisionService(BaseService):
    def get_by_id(self, engine_id: str, engine_revision_id: str) -> EngineRevision:
        """
        Get an EngineRevision from Firebolt by engine_id and engine_revision_id.
        """

        return self.get_by_key(
            EngineRevisionKey(
                account_id=self.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    def get_by_key(self, key: EngineRevisionKey) -> EngineRevision:
        """
        Fetch an EngineRevision from Firebolt by its key.

        Args:
            key: Key of the desired EngineRevision

        Returns:
            The requested EngineRevision
        """

        response = self.client.get(
            url=ACCOUNT_ENGINE_REVISION_URL.format(
                account_id=key.account_id,
                engine_id=key.engine_id,
                revision_id=key.engine_revision_id,
            ),
        )
        engine_spec: dict = response.json()["engine_revision"]
        return EngineRevision.parse_obj(engine_spec)
