import struct
import uuid
from array import array

import pytest

from aiokdb import (
    AttrEnum,
    TypeEnum,
    b9,
    cv,
    d9,
    ka,
    kb,
    kc,
    kg,
    kh,
    ki,
    kj,
    kk,
    krr,
    ks,
    ktn,
    xd,
    xt,
)
from aiokdb.extras import ktnb, ktni, ktns, ktnu


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
    assert b9(kb(False)) == h2b("0x010000000a000000ff00")
    assert b9(kb(True)) == h2b("0x010000000a000000ff01")
    assert b9(kc(" ")) == h2b("0x010000000a000000f620")


def test_atoms_d9b9() -> None:
    assert d9(b9(kb(True))).aB() is True
    assert d9(b9(kb(False))).aB() is False
    assert d9(b9(kg(12))).aG() == 12
    assert d9(b9(kh(12))).aH() == 12
    assert d9(b9(ki(12))).aI() == 12
    assert d9(b9(kj(12))).aJ() == 12
    assert d9(b9(kc(" "))).aC() == " "


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

    k2 = ktni(TypeEnum.KH, 3, 2)
    assert k == k2

    k = ktn(TypeEnum.KC)
    k.kC().fromunicode("2+2")
    assert b9(k) == h2b("0x01000000110000000a0003000000322b32")

    # -8!2#0Ng
    k = ktn(TypeEnum.UU, sz=2)
    assert b9(k) == h2b(
        "0x010000002e0000000200020000000000000000000000000000000000000000000000000000000000000000000000"
    )


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

    x = d9(h2b("0x010000001a000000000002000000000000000000000000000000"))
    assert len(x.kK()) == 2
    assert x.kK()[0].kK() == []


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

    with pytest.raises(struct.error, match=r"(.*ubyte.*|.*format requires.*)"):
        kg(-1)
    with pytest.raises(struct.error, match=r"(.*ubyte.*|.*format requires.*)"):
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
    with pytest.raises(struct.error, match=r".*format requires .* <= number .*"):
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

    k = kk(ks("key1"), ks("key2"))
    v = kk(kj(1), kj(2))
    d = xd(k, v)
    assert len(d) == 2
    d["key1"].t == TypeEnum.KJ
    assert d["key1"].aJ() == 1
    assert d.kS() == ["key1", "key2"]

    k = ktn(TypeEnum.KH, sz=2)
    v = ktn(TypeEnum.KH, sz=1)
    with pytest.raises(ValueError, match="dict keys and values must be same length"):
        xt(xd(k, v))


def test_table_checks() -> None:
    # blocked by type system
    # with pytest.raises(ValueError, match="can only flip a dict"):
    #    xt(kj(5))

    k = ktn(TypeEnum.KH)
    v = ktn(TypeEnum.KH)
    with pytest.raises(ValueError, match="must have >0 columns"):
        xt(xd(k, v))

    # you can build these dictionaries, but not flip them as not symbols
    k = ktn(TypeEnum.KH, sz=2)
    v = ktn(TypeEnum.KH, sz=2)
    d = xd(k, v)
    with pytest.raises(ValueError, match="must have K vector holding cols"):
        xt(d)

    k = ktn(TypeEnum.KH, sz=2)
    v = kk(ktn(TypeEnum.KJ, sz=3), ktn(TypeEnum.KJ, sz=3))
    d = xd(k, v)
    with pytest.raises(ValueError, match="dict key must be S vector of column names"):
        xt(d)

    # dict has values with different lengths
    k = ktn(TypeEnum.KS, sz=2)
    v = kk(ktn(TypeEnum.KJ, sz=3), ktn(TypeEnum.KJ, sz=4))
    with pytest.raises(ValueError, match="column length inconsistent"):
        xt(xd(k, v))

    k = ktn(TypeEnum.KS, sz=2)
    v = kk(ktn(TypeEnum.KJ, sz=3), ktn(TypeEnum.KJ, sz=3))
    t = xt(xd(k, v))
    assert len(t) == 3

    # zero length table
    v = kk(ktn(TypeEnum.KJ), ktn(TypeEnum.KJ))
    t = xt(xd(k, v))
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
    # there is one row in this table
    assert len(t) == 1

    # shortcuts
    assert t.kS() == ["a", "b"]
    assert t.kK()[0].kI() == array("l", [2])
    assert t["b"].kI() == array("l", [3])

    # accessing rows as a slice gives a table

    # accessing rows by int gives a dictionary
    d = t[0]
    assert d.t == TypeEnum.XD
    assert d.kS() == ["a", "b"]  # keys same
    d["a"].aI() == 2
    d["b"].aI() == 3
    with pytest.raises(IndexError):
        t[1]


def test_identity() -> None:
    k = d9(h2b("0x010000000a0000006500"))
    assert k.t == TypeEnum.NIL

    k = ka(TypeEnum.NIL)
    assert b2h(b9(k)) == "0x010000000a0000006500"

    with pytest.raises(Exception, match=r"wrong type NIL \(101\) for aJ"):
        k.aJ()

    with pytest.raises(Exception, match=r"Not available for NIL \(101\)"):
        k.kJ()


def test_mixed() -> None:
    # symbol, long, symbol, bool
    k = ktn(TypeEnum.K)
    k.kK().extend([ks("function"), kj(17), ks("XBT"), kb(False)])

    k2 = kk(ks("function"), kj(17), ks("XBT"), kb(False))
    assert b9(k) == b9(k2)


def test_equals() -> None:
    assert kj(34) == kj(34)
    assert kj(34) != kj(35)


def test_table_uuid_str_column() -> None:
    # x:([envelope_id:"G"$("2d948578-e9d6-79a2-8207-9df7a71f0b3b";"409031f3-b19c-6770-ee84-6e9369c98697")]payload:("abc";"xy");time:2024.05.14D23:13:19.044908000)
    # envelope_id                         | payload time
    # ------------------------------------| -------------------------------------
    # 2d948578-e9d6-79a2-8207-9df7a71f0b3b| "abc"   2024.05.14D23:13:19.044908000
    # 409031f3-b19c-6770-ee84-6e9369c98697| "xy"    2024.05.14D23:13:19.044908000
    # -8!x
    exp = h2b(
        "0x0100000093000000636200630b0001000000656e76656c6f70655f6964000000010000000200020000002d948578e9d679a282079df7a71f0b3b409031f3b19c6770ee846e9369c986976200630b00020000007061796c6f61640074696d65000000020000000000020000000a00030000006162630a000200000078790c0002000000e013dc291431ac0ae013dc291431ac0a"
    )

    # keyed table
    # dictionary with tables for keys and values
    key_hdr = ktns(TypeEnum.KS, "envelope_id")
    key_val = kk(ktn(TypeEnum.UU))

    val_hdr = ktns(TypeEnum.KS, "payload", "time")
    val_val = kk(ktn(TypeEnum.K), ktn(TypeEnum.KP))
    kt = xd(xt(xd(key_hdr, key_val)), xt(xd(val_hdr, val_val)))

    # add items
    kt.kkey()["envelope_id"].kU().append(
        uuid.UUID("2d948578-e9d6-79a2-8207-9df7a71f0b3b")
    )
    kt.kvalue()["payload"].kK().append(cv("abc"))
    kt.kvalue()["time"].kJ().append(769043599044908000)

    kt.kkey()["envelope_id"].kU().append(
        uuid.UUID("409031f3-b19c-6770-ee84-6e9369c98697")
    )
    kt.kvalue()["payload"].kK().append(cv("xy"))
    kt.kvalue()["time"].kJ().append(769043599044908000)
    assert b9(kt).hex() == exp.hex()


def test_vector_extras() -> None:
    assert d9(b9(ktni(TypeEnum.KP, 769043599044908000))).kJ()[0] == 769043599044908000

    a = uuid.uuid4()
    b = uuid.uuid4()
    assert d9(b9(ktnu(a, b))).kU() == [a, b]

    assert d9(b9(ktnb(True, False, True))).kB() == [True, False, True]


def test_kk() -> None:
    with pytest.raises(ValueError, match="not KObj: None"):
        kk(kj(4), None)  # type: ignore
