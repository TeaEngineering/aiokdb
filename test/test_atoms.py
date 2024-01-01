import pytest

from aiokdb import KContext, TypeEnum, b9, ki, kj, ks, ktn, xd


def h2b(hx: str) -> bytes:
    assert hx[0:2] == "0x"
    return bytes.fromhex(hx[2:])


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
    with pytest.raises(ValueError):
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


def test_serialization_examples() -> None:
    # Examples provided at https://code.kx.com/q/kb/serialization/
    # under CC-A4 by Kx Systems., Inc.

    # integer of value 1
    # q)-8!1
    assert b9(ki(1)) == h2b("0x010000000d000000fa01000000")

    # integer vector
    # q)-8!enlist 1
    k = ktn(TypeEnum.KI, 1)
    k.kI()[0] = 1
    assert b9(k) == h2b("0x010000001200000006000100000001000000")

    # byte vector
    # q)-8!`byte$til 5
    k = ktn(TypeEnum.KG)
    k.kB().extend(range(5))
    assert b9(k) == h2b("0x01000000130000000400050000000001020304")

    # general list
    # q)-8!enlist`byte$til 5
    k2 = ktn(0)
    k2.kK().append(k)
    assert b9(k2) == h2b("0x01000000190000000000010000000400050000000001020304")

    # dictionary with atom values
    # -8!`a`b!2 3i
    ks = ktn(TypeEnum.KS)
    ks.appendS("a", "b")  # uses an assumed global context for enumeration
    kv = ktn(TypeEnum.KI)
    kv.kI().extend([2, 3])
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x0100000021000000630b0002000000610062000600020000000200000003000000"
    )

    # dictionary with atom values
    # -8!`a`b!2 3
    kv = ktn(TypeEnum.KJ)
    kv.kJ().extend([2, 3])
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x0100000029000000630b00020000006100620007000200000002000000000000000300000000000000"
    )

    # sorted/stepped dictionary with atom values (encoding of flags field)
    # has both sorted dict (7f) and s-meta on keys vector (01)
    # q)-8!`s#`a`b!2 3i
    ks = ktn(TypeEnum.KS, sorted=True)
    ks.appendS("a", "b")  # uses an assumed global context for enumeration
    kv = ktn(TypeEnum.KI)
    kv.kI().extend([2, 3])
    k = xd(ks, kv, sorted=True)
    assert b9(k) == h2b(
        "0x01000000210000007f0b0102000000610062000600020000000200000003000000"
    )

    # dictionary with vector values
    # q)-8!`a`b!enlist each 2 3
    ks = ktn(TypeEnum.KS)
    ks.appendS("a", "b")  # uses an assumed global context for enumeration
    kv = ktn(TypeEnum.K)
    kv.kK().append(ktn(TypeEnum.KI))
    kv.kK().append(ktn(TypeEnum.KI))
    kv.kK()[0].kI().append(2)
    kv.kK()[1].kI().append(3)
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000"
    )
