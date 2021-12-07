from asyncio import run

from pytest import raises

from firebolt.common.util import async_to_sync


def test_async_to_sync():
    class JobMarker(Exception):
        pass

    async def task():
        raise JobMarker()

    with raises(JobMarker):
        run(task())

    # Here local event loop is closed by run

    with raises(JobMarker):
        async_to_sync(task)()
