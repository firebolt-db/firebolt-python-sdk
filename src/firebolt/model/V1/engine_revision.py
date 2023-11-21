from __future__ import annotations

from firebolt.model.V1 import FireboltBaseModel


class EngineRevisionKey(FireboltBaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str
