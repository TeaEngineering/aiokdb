import pytest

from aiokdb import KContext


def test_context() -> None:
    kcon = KContext()
    assert kcon.ss("hello") == 0
    assert kcon.ss("world") == 1
    assert kcon.ss("hello") == 0
    assert kcon.ss("world") == 1
    assert kcon.ss("how") == 2
    with pytest.raises(ValueError):
        kcon.ss("💩")
    assert len(kcon.symbols) == 3
