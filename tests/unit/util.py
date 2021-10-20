from typing import Dict, List

from firebolt.model import FireboltBaseModel


def list_to_paginated_response(items: List[FireboltBaseModel]) -> Dict:
    return {"edges": [{"node": i.dict()} for i in items]}
