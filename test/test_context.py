from aiokdb.context import KContext


def test_context() -> None:
    kcon = KContext()
    assert kcon.ss("hello") == 0
    assert kcon.ss("world") == 1
    assert kcon.ss("hello") == 0
    assert kcon.ss("world") == 1
    assert kcon.ss("how") == 2
    assert kcon.ss("💩") == 3
    assert len(kcon.symbols) == 4

    assert kcon.lookup_str(2) == "how"
    assert kcon.lookup_bytes(2) == b"how\00"
