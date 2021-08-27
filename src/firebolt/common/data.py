from toolz.dicttoolz import valfilter


def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None"""
    return valfilter(lambda x: x is not None, d)
