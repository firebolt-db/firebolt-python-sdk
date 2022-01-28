from asyncio import (
    AbstractEventLoop,
    get_event_loop,
    new_event_loop,
    set_event_loop,
)
from functools import lru_cache, wraps
from threading import Thread
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Optional,
    Type,
    TypeVar,
)

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


class AsyncJobThread:
    """
    Thread runner that allows running async tasks syncronously in a separate thread.
    Caches loop to be reused in all threads
    It allows running async functions syncronously inside a running event loop.
    Since nesting loops is not allowed, we create a separate thread for a new event loop
    """

    def __init__(self) -> None:
        self.loop: Optional[AbstractEventLoop] = None
        self.result: Optional[Any] = None
        self.exception: Optional[BaseException] = None

    def _initialize_loop(self) -> None:
        if not self.loop:
            try:
                # despite the docs, this function fails if no loop is set
                self.loop = get_event_loop()
            except RuntimeError:
                self.loop = new_event_loop()
        set_event_loop(self.loop)

    def run(self, coro: Coroutine) -> None:
        try:
            self._initialize_loop()
            assert self.loop is not None
            self.result = self.loop.run_until_complete(coro)
        except BaseException as e:
            self.exception = e

    def execute(self, coro: Coroutine) -> Any:
        thread = Thread(target=self.run, args=[coro])
        thread.start()
        thread.join()
        if self.exception:
            raise self.exception
        return self.result


def async_to_sync(f: Callable, async_job_thread: AsyncJobThread = None) -> Callable:
    @wraps(f)
    def sync(*args: Any, **kwargs: Any) -> Any:
        try:
            loop = get_event_loop()
        except RuntimeError:
            loop = new_event_loop()
            set_event_loop(loop)
        # We are inside a running loop
        if loop.is_running():
            nonlocal async_job_thread
            if not async_job_thread:
                async_job_thread = AsyncJobThread()
            return async_job_thread.execute(f(*args, **kwargs))
        return loop.run_until_complete(f(*args, **kwargs))

    return sync
