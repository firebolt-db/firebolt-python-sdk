from firebolt.model import FireboltBaseModel


def list_to_paginated_response(items: list[FireboltBaseModel]) -> dict:
    return {"edges": [{"node": i.dict()} for i in items]}
