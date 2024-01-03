import struct
import uuid
from array import array

import pytest

from aiokdb import AttrEnum, KContext, TypeEnum, b9, d9, kg, ki, kj, ks, ktn, xd, xt


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
    with pytest.raises(ValueError, match=r".*wrong type KI \(-6\) for aJ"):
        ki(1).aJ()
    with pytest.raises(ValueError, match=r".*wrong type KJ \(-7\) for aI"):
        kj(1).aI()

    # enumerates and stores the index, then reverses the lookup
    assert ks("hello").aS() == "hello"


def test_atoms_b9() -> None:
    assert b9(ki(-1)) == h2b("0x010000000d000000faffffffff")
    # q)-8!`abc   to check null termination of symbols
    assert b9(ks("abc")) == h2b("0x010000000d000000f561626300")
    assert b9(ks("abcd")) == h2b("0x010000000e000000f56162636400")


def test_atoms_d9() -> None:
    with pytest.raises(struct.error, match=r".*at least 8 bytes.*"):
        d9(h2b("0x01"))
    with pytest.raises(ValueError, match=r".*required 13 bytes, got 12"):
        d9(h2b("0x010000000d000000faffffff"))

    assert d9(h2b("0x010000000a000000ff00")).aB() is False  # -8!0b
    assert d9(h2b("0x010000000a000000ff01")).aB() is True  # -8!1b
    assert d9(h2b("0x010000000a000000fc02")).aG() == 2  # -8!0x2
    assert d9(h2b("0x010000000a000000fcff")).aG() == 255  # -8!0xff
    assert d9(h2b("0x010000000b000000fb0200")).aH() == 2
    assert d9(h2b("0x010000000b000000fbffff")).aH() == -1
    assert d9(h2b("0x010000000d000000fa02000000")).aI() == 2
    assert d9(h2b("0x010000000d000000faffffffff")).aI() == -1
    assert d9(h2b("0x0100000011000000f90200000000000000")).aJ() == 2
    assert d9(h2b("0x0100000011000000f9ffffffffffffffff")).aJ() == -1
    # -8!"G"$"97ebf398-b01a-0870-b5b7-8fc9e4edd95a"
    assert d9(
        h2b("0x0100000019000000fe97ebf398b01a0870b5b78fc9e4edd95a")
    ).aU() == uuid.UUID("97ebf398-b01a-0870-b5b7-8fc9e4edd95a")

    # q real / python float
    assert d9(h2b("0x010000000d000000f89a995940")).aE() == pytest.approx(
        3.4, 0.0001
    )  # -8!3.4e
    assert d9(h2b("0x010000000d000000f89a9959c0")).aE() == pytest.approx(
        -3.4, 0.0001
    )  # -8!-3.4e
    # q float / python double
    assert d9(h2b("0x0100000011000000f73333333333330b40")).aF() == pytest.approx(
        3.4, 0.0001
    )
    assert d9(h2b("0x0100000011000000f73333333333330bc0")).aF() == pytest.approx(
        -3.4, 0.0001
    )

    assert d9(h2b("0x010000000a000000f643")).aC() == "C"  # -8!"C"
    # single byte non-printable char
    assert d9(h2b("0x010000000a000000f60f")).aC() == "\x0f"
    # symbol -- null terminated parsing
    assert d9(h2b("0x010000000d000000f561626300")).aS() == "abc"


def test_vector_b9() -> None:
    k = ktn(TypeEnum.KH)
    k.kH().extend([3, 2])
    assert b9(k) == h2b("0x010000001200000005000200000003000200")


def test_vector_d9() -> None:
    # assert d9(h2b("0x010000001200000001000400000000010100")).kB() == [False, True, True, False]
    assert d9(h2b("0x0100000011000000040003000000c0ffee")).kG() == array(
        "B", [192, 255, 238]
    )


def test_overflows_KG() -> None:
    k = ktn(TypeEnum.KG, attr=AttrEnum.SORTED)
    with pytest.raises(
        OverflowError, match="unsigned byte integer is greater than maximum"
    ):
        k.kG().append(256)
    with pytest.raises(
        OverflowError, match="unsigned byte integer is less than minimum"
    ):
        k.kG().append(-1)
    k.kG().append(255)
    assert len(k.kG()) == 1

    with pytest.raises(struct.error, match=r".*ubyte.*"):
        kg(-1)
    with pytest.raises(struct.error, match=r".*ubyte.*"):
        kg(256)
    kg(255)
    kg(0)


def test_overflows_h() -> None:
    k = ktn(TypeEnum.KH, attr=AttrEnum.SORTED)
    with pytest.raises(
        OverflowError, match="signed short integer is greater than maximum"
    ):
        k.kH().append(80909)
    with pytest.raises(
        OverflowError, match="signed short integer is less than minimum"
    ):
        k.kH().append(-80912)
    k.kH().append(8)
    assert len(k.kH()) == 1


def test_dict_checks() -> None:
    k = ktn(TypeEnum.KH)
    v = ktn(TypeEnum.KH)
    d = xd(k, v)
    assert len(d) == 0


def test_table_checks() -> None:
    k = ktn(TypeEnum.KH)
    v = ktn(TypeEnum.KH)
    d = xd(k, v)
    t = xt(d)
    assert len(t) == 0
