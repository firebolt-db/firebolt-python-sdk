def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None"""
    return {k: v for k, v in d.items() if v is not None}
