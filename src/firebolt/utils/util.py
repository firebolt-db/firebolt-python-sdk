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

from httpx import URL

T = TypeVar("T")


def cached_property(func: Callable[..., T]) -> T:
    """cached_property implementation for 3.7 backward compatibility.

    Args:
        func (Callable): Property getter

    Returns:
        T: Property of type, returned by getter
    """
    return property(lru_cache()(func))  # type: ignore


def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None.

    Args:
        d (dict): Dict to prune

    Returns:
        dict: Pruned dict
    """
    return {k: v for k, v in d.items() if v is not None}


TMix = TypeVar("TMix")


def mixin_for(baseclass: Type[TMix]) -> Type[TMix]:
    """Define mixin with baseclass typehint.

    Should be used as a mixin base class to fix typehints.

    Args:
        baseclass (Type[TMix]): Class which mixin will be made for

    Returns:
        Type[TMix]: Mixin type to inherit from

    Examples:
        ```
        class ReadonlyMixin(mixin_for(BaseClass))):
            ...
        ```

    """
    if TYPE_CHECKING:
        return baseclass
    return object


def fix_url_schema(url: str) -> str:
    """Add schema to URL if it's missing.

    Args:
        url (str): URL to check

    Returns:
        str: URL with schema present

    """
    return url if url.startswith("http") else f"https://{url}"


class AsyncJobThread:
    """Thread runner that allows running async tasks synchronously in a separate thread.

    Caches loop to be reused in all threads.
    It allows running async functions synchronously inside a running event loop.
    Since nesting loops is not allowed, we create a separate thread for a new event loop

    Attributes:
        result (Any): Value, returned by coroutine execution
        exception (Optional[BaseException]): If any, exception that occurred
            during coroutine execution
    """

    def __init__(self) -> None:
        self._loop: Optional[AbstractEventLoop] = None
        self.result: Any = None
        self.exception: Optional[BaseException] = None

    def _initialize_loop(self) -> None:
        """Initialize a loop once to use for later execution.

        Tries to get a running loop.
        Creates a new loop if no active one, and sets it as active.
        """
        if not self._loop:
            try:
                # despite the docs, this function fails if no loop is set
                self._loop = get_event_loop()
            except RuntimeError:
                self._loop = new_event_loop()
        set_event_loop(self._loop)

    def _run(self, coro: Coroutine) -> None:
        """Run coroutine in an event loop.

        Execution return value is stored into ``result`` field.
        If an exception occurs, it will be caught and stored into ``exception`` field.

        Args:
            coro (Coroutine): Coroutine to execute
        """
        try:
            self._initialize_loop()
            assert self._loop is not None
            self.result = self._loop.run_until_complete(coro)
        except BaseException as e:
            self.exception = e

    def execute(self, coro: Coroutine) -> Any:
        """Execute coroutine in a separate thread.

        Args:
            coro (Coroutine): Coroutine to execute

        Returns:
            Any: Coroutine execution return value

        Raises:
            exception: Exeption, occured within coroutine
        """
        thread = Thread(target=self._run, args=[coro])
        thread.start()
        thread.join()
        if self.exception:
            raise self.exception
        return self.result


def async_to_sync(f: Callable, async_job_thread: AsyncJobThread = None) -> Callable:
    """Convert async function to sync.

    Args:
        f (Callable): function to convert
        async_job_thread (AsyncJobThread): Job thread instance to use for async excution
            (Default value = None)

    Returns:
        Callable: regular function, which can be executed synchronously
    """

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


def merge_urls(base: URL, merge: URL) -> URL:
    """Merge a base and merge urls.

    If merge is not a relative url, do nothing

    Args:
        base (URL): Base URL to merge to
        merge (URL): URL to merge

    Returns:
        URL: Resulting URL
    """
    if merge.is_relative_url:
        merge_raw_path = base.raw_path + merge.raw_path.lstrip(b"/")
        return base.copy_with(raw_path=merge_raw_path)
    return merge
