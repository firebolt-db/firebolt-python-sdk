from functools import lru_cache
from typing import Callable, TypeVar

T = TypeVar("T")


def cached_property(func: Callable[..., T]) -> T:
    return property(lru_cache()(func))  # type: ignore
