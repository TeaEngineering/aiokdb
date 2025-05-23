import asyncio

import pytest

from aiokdb import KObj
from aiokdb.server import ConnectionClosed, KdbReader, KdbWriter


class DummyWriter(asyncio.StreamWriter):
    def __init__(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return True

    async def wait_closed(self) -> None:
        pass


class DummyReader(KdbReader):
    def __init__(self) -> None:
        pass


@pytest.mark.asyncio
async def test_kdbwriter_close_sets_exception_on_pending_futures() -> None:
    writer = KdbWriter(DummyWriter(), DummyReader())
    fut1: asyncio.Future[KObj] = asyncio.Future()
    fut2: asyncio.Future[KObj] = asyncio.Future()
    writer._completions.append(fut1)
    writer._completions.append(fut2)
    writer.close()
    # Both futures should now be done and have ConnectionClosed exception
    for fut in (fut1, fut2):
        assert fut.done()
        with pytest.raises(ConnectionClosed):
            fut.result()
