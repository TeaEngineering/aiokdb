import struct
import uuid
from array import array

import pytest

from aiokdb import AttrEnum, TypeEnum, b9, d9, ka, kg, kh, ki, kj, krr, ks, ktn, xd, xt


def h2b(hx: str) -> bytes:
    assert hx[0:2] == "0x"
    return bytes.fromhex(hx[2:])


def b2h(bs: bytes) -> str:
    return "0x" + bs.hex()


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
    assert b9(krr("ohno")) == h2b("0x010000000e000000806F686E6F00")


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
    # -8!3.4e
    assert d9(h2b("0x010000000d000000f89a995940")).aE() == pytest.approx(3.4, 0.0001)
    # -8!-3.4e
    assert d9(h2b("0x010000000d000000f89a9959c0")).aE() == pytest.approx(-3.4, 0.0001)
    # q float / python double
    assert d9(h2b("0x0100000011000000f73333333333330b40")).aF() == pytest.approx(
        3.4, 0.001
    )
    assert d9(h2b("0x0100000011000000f73333333333330bc0")).aF() == pytest.approx(
        -3.4, 0.001
    )

    assert d9(h2b("0x010000000a000000f643")).aC() == "C"  # -8!"C"
    # single byte non-printable char
    assert d9(h2b("0x010000000a000000f60f")).aC() == "\x0f"
    # symbol -- null terminated parsing
    assert d9(h2b("0x010000000d000000f561626300")).aS() == "abc"
    # KRR remote error

    k = d9(h2b("0x010000000e000000806F686E6F00"))
    assert k.t == TypeEnum.KRR
    assert k.aS() == "ohno"


def test_vector_b9() -> None:
    k = ktn(TypeEnum.KH)
    k.kH().extend([3, 2])
    assert b9(k) == h2b("0x010000001200000005000200000003000200")

    k = ktn(TypeEnum.KC)
    k.kC().fromunicode("2+2")
    assert b9(k) == h2b("0x01000000110000000a0003000000322b32")


def test_vector_d9() -> None:
    # assert d9(h2b("0x010000001200000001000400000000010100")).kB() == [False, True, True, False]
    # q)-8!0xC0FFEE
    assert d9(h2b("0x0100000011000000040003000000c0ffee")).kG() == array(
        "B", [192, 255, 238]
    )
    # q)-8!3 4 5 6h
    assert d9(h2b("0x01000000160000000500040000000300040005000600")).kH() == array(
        "h", [3, 4, 5, 6]
    )
    # q)-8!3 4 5 6i
    assert d9(
        h2b("0x010000001e00000006000400000003000000040000000500000006000000")
    ).kI() == array("l", [3, 4, 5, 6])
    # q)-8!3 4 5 6j
    assert d9(
        h2b(
            "0x010000002e0000000700040000000300000000000000040000000000000005000000000000000600000000000000"
        )
    ).kJ() == array("q", [3, 4, 5, 6])
    # char vector
    assert d9(h2b("0x01000000110000000a0003000000322b32")).kC() == array("u", "2+2")

    # sym vector
    # q)-8!`ab`c`defghijklmnopq`rstuvwxy`z
    assert d9(
        h2b(
            "0x010000002d0000000b000500000061620063006465666768696a6b6c6d6e6f7071007273747576777879007a00"
        )
    ).kS() == ["ab", "c", "defghijklmnopq", "rstuvwxy", "z"]

    # GUID vector
    # q)-8!2#"G"$"97ebf398-b01a-0870-b5b7-8fc9e4edd95a"
    assert d9(
        h2b(
            "0x010000002e00000002000200000097ebf398b01a0870b5b78fc9e4edd95a97ebf398b01a0870b5b78fc9e4edd95a"
        )
    ).kU() == [
        uuid.UUID("97ebf398-b01a-0870-b5b7-8fc9e4edd95a"),
        uuid.UUID("97ebf398-b01a-0870-b5b7-8fc9e4edd95a"),
    ]


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


def test_overflows_KH() -> None:
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

    kh(0)
    kh(32767)
    kh(-32767)
    kh(-32768)  # null
    with pytest.raises(struct.error, match=r"short format requires .* <= number .*"):
        kh(32768)


def test_dict_d9() -> None:
    # q)-8!d:`a`b`c!(1 2i;3 5 9i;enlist 7i)
    k = d9(
        h2b(
            "0x0100000045000000630b0003000000610062006300000003000000060002000000010000000200000006000300000003000000050000000900000006000100000007000000"
        )
    )
    assert k.t == TypeEnum.XD
    assert k.kkey().t == TypeEnum.KS
    assert k.kvalue().t == TypeEnum.K
    assert len(k) == 3
    assert k.kkey().kS() == ["a", "b", "c"]
    assert k.kvalue().kK()[0].t == TypeEnum.KI
    assert len(k.kvalue().kK()[0]) == 2
    assert k.kvalue().kK()[0].kI() == array("l", [1, 2])
    assert k["a"].kI() == array("l", [1, 2])
    with pytest.raises(KeyError):
        k["z"]


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


def test_table_d9() -> None:
    t = d9(
        h2b(
            "0x010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000"
        )
    )
    assert t.t == TypeEnum.XT
    d = t.kvalue()
    assert d.t == TypeEnum.XD
    assert d.kkey().kS() == ["a", "b"]
    As, Bs = d.kvalue().kK()
    assert As.kI() == array("l", [2])
    assert Bs.kI() == array("l", [3])


def test_identity() -> None:
    k = d9(h2b("0x010000000a0000006500"))
    assert k.t == TypeEnum.NIL

    k = ka(TypeEnum.NIL)
    assert b2h(b9(k)) == "0x010000000a0000006500"
