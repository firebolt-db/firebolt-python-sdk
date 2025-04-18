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


async def _anext(iterator: AsyncIterator[TIter], default: TIter) -> TIter:
    try:
        return await iterator.__anext__()
    except StopAsyncIteration:
        return default


# Built-in anext is only available in Python 3.11 and above
anext = __builtins__.anext if hasattr(__builtins__, "anext") else _anext
