from functools import lru_cache
from typing import TYPE_CHECKING, Callable, Type, TypeVar

T = TypeVar("T")


def cached_property(func: Callable[..., T]) -> T:
    return property(lru_cache()(func))  # type: ignore


def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None"""
    return {k: v for k, v in d.items() if v is not None}


TMix = TypeVar("TMix")


def mixin_for(baseclass: Type[TMix]) -> Type[TMix]:
    """
    Useful function to make mixins with baseclass typehint

    ```
    class ReadonlyMixin(mixin_for(BaseAdmin))):
        ...
    ```
    """
    if TYPE_CHECKING:
        return baseclass
    return object
