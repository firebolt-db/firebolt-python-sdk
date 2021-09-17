from firebolt.model import FireboltBaseModel


def paginated(items: list[FireboltBaseModel]) -> dict:
    return {"edges": [{"node": i.dict()} for i in items]}
