from asyncio import run
from threading import Thread

from pytest import mark, raises

from firebolt.utils.util import async_to_sync


def test_async_to_sync_happy_path():
    """async_to_sync properly converts coroutine to sync function"""

    class JobMarker(Exception):
        pass

    async def task():
        raise JobMarker()

    for i in range(3):
        with raises(JobMarker):
            async_to_sync(task)()


def test_async_to_sync_thread():
    """async_to_sync properly works in threads"""

    marks = [False] * 3

    async def task(id: int):
        marks[id] = True

    ts = [Thread(target=async_to_sync(task), args=[i]) for i in range(3)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(marks)


def test_async_to_sync_after_run():
    """async_to_sync properly runs after asyncio.run"""

    class JobMarker(Exception):
        pass

    async def task():
        raise JobMarker()

    with raises(JobMarker):
        run(task())

    # Here local event loop is closed by run

    with raises(JobMarker):
        async_to_sync(task)()


@mark.asyncio
async def test_nested_loops() -> None:
    """async_to_sync properly works inside a running loop"""

    class JobMarker(Exception):
        pass

    async def task():
        raise JobMarker()

    with raises(JobMarker):
        await task()

    with raises(JobMarker):
        async_to_sync(task)()
