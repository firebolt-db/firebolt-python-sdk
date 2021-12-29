from asyncio import get_event_loop, new_event_loop, set_event_loop
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Any, Callable, Type, TypeVar

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
    Should be used as a mixin base class to fix typehints

    ```
    class ReadonlyMixin(mixin_for(BaseClass))):
        ...
    ```
    """

    if TYPE_CHECKING:
        return baseclass
    return object


def fix_url_schema(url: str) -> str:
    return url if url.startswith("http") else f"https://{url}"


def async_to_sync(f: Callable) -> Callable:
    @wraps(f)
    def sync(*args: Any, **kwargs: Any) -> Any:
        try:
            loop = get_event_loop()
        except RuntimeError:
            loop = new_event_loop()
            set_event_loop(loop)
        res = loop.run_until_complete(f(*args, **kwargs))
        return res

    return sync
