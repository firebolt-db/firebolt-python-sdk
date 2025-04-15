from typing import AsyncIterator, List, TypeVar

TIter = TypeVar("TIter")


async def async_islice(async_iterator: AsyncIterator[TIter], n: int) -> List[TIter]:
    result = []
    try:
        for _ in range(n):
            result.append(await async_iterator.__anext__())
    except StopAsyncIteration:
        pass
    return result
