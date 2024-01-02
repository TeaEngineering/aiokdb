import pytest

from aiokdb import AttrEnum, KContext, TypeEnum, b9, ki, kj, ks, ktn, xd, xt


def h2b(hx: str) -> bytes:
    assert hx[0:2] == "0x"
    return bytes.fromhex(hx[2:])


def b2h(bs: bytes) -> str:
    return "0x" + bs.hex()


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


def test_atoms_round_trip() -> None:
    assert ki(1).aI() == 1
    assert kj(1).aJ() == 1
    # mixing setters and getters of different widths is a bug
    with pytest.raises(ValueError, match=r".*wrong type KI \(-6\) for kJ"):
        ki(1).aJ()

    # enumerates and stores the index, then reverses the lookup
    assert ks("hello").aS() == "hello"


def test_atoms_b9() -> None:
    assert b9(ki(-1)) == h2b("0x010000000d000000faffffffff")
    # q)-8!`abc   to check null termination of symbols
    assert b9(ks("abc")) == h2b("0x010000000d000000f561626300")
    assert b9(ks("abcd")) == h2b("0x010000000e000000f56162636400")


def test_vector_b9() -> None:
    k = ktn(TypeEnum.KH)
    k.kH().extend([3, 2])
    assert b9(k) == h2b("0x010000001200000005000200000003000200")
