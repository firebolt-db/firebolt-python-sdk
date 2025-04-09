from typing import AsyncIterator, List, TypeVar

from firebolt.common.row_set.types import AsyncByteStream

TIter = TypeVar("TIter")


async def async_islice(async_iterator: AsyncIterator[TIter], n: int) -> List[TIter]:
    result = []
    try:
        for _ in range(n):
            result.append(await async_iterator.__anext__())
    except StopAsyncIteration:
        pass
    return result


def async_byte_stream(b: bytes) -> AsyncByteStream:
    class ABS:
        def __init__(self, b: bytes):
            self.b = b
            self.read = False

        def __aiter__(self) -> AsyncIterator[bytes]:
            return self

        async def __anext__(self) -> bytes:
            if self.read:
                raise StopAsyncIteration
            self.read = True
            return self.b

        async def aclose(self) -> None:
            # No-op since there is nothing to close
            pass

    return ABS(b)
