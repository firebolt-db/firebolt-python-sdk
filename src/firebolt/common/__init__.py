from firebolt.common.exception import (
    AlreadyBoundError,
    DatabaseRequiredError,
    EndpointRequiredError,
    FireboltClientRequiredError,
    FireboltEngineError,
    FireboltError,
)
from firebolt.common.settings import Settings


def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None"""
    return {k: v for k, v in d.items() if v is not None}
