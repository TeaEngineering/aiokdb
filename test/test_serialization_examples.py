from aiokdb import AttrEnum, TypeEnum, b9, ki, ktn, xd, xt


def h2b(hx: str) -> bytes:
    assert hx[0:2] == "0x"
    return bytes.fromhex(hx[2:])


def b2h(bs: bytes) -> str:
    return "0x" + bs.hex()


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
    k.kG().extend(range(5))
    assert b9(k) == h2b("0x01000000130000000400050000000001020304")

    # general list
    # q)-8!enlist`byte$til 5
    k2 = ktn(TypeEnum.K)
    k2.kK().append(k)
    assert b9(k2) == h2b("0x01000000190000000000010000000400050000000001020304")

    # dictionary with atom values
    # q)-8!`a`b!2 3i
    ks = ktn(TypeEnum.KS)
    ks.appendS("a", "b")  # uses an assumed global context for enumeration
    kv = ktn(TypeEnum.KI)
    kv.kI().extend([2, 3])
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x0100000021000000630b0002000000610062000600020000000200000003000000"
    )

    # dictionary with atom values
    # q)-8!`a`b!2 3
    kv = ktn(TypeEnum.KJ)
    kv.kJ().extend([2, 3])
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x0100000029000000630b00020000006100620007000200000002000000000000000300000000000000"
    )

    # sorted/stepped dictionary with atom values (encoding of flags field)
    # has both sorted dict (7f) and s-meta on keys vector (01)
    # q)-8!`s#`a`b!2 3i
    ksorted = ktn(TypeEnum.KS, attr=AttrEnum.SORTED)
    ksorted.appendS("a", "b")
    kv = ktn(TypeEnum.KI)
    kv.kI().extend([2, 3])
    k = xd(ksorted, kv, sorted=True)
    assert b9(k) == h2b(
        "0x01000000210000007f0b0102000000610062000600020000000200000003000000"
    )

    # dictionary with vector values
    # q)-8!`a`b!enlist each 2 3i
    kv = ktn(TypeEnum.K)
    kv.kK().append(ktn(TypeEnum.KI))
    kv.kK().append(ktn(TypeEnum.KI))
    kv.kK()[0].kI().append(2)
    kv.kK()[1].kI().append(3)
    k = xd(ks, kv)
    assert b9(k) == h2b(
        "0x010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000"
    )

    # table
    # q)-8!([]a:enlist 2i;b:enlist 3i)
    # q)flip `a`b!enlist each 2 3i
    t = xt(k)
    assert b9(t) == h2b(
        "0x010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000"
    )

    # sorted table
    # q)-8!`s#([]a:enlist 2i;b:enlist 3i)
    # in KDB this magically sets the parted bit on the first column (?!)
    t = xt(k, sorted=True)
    t.kvalue().kvalue().kK()[0].attrib = AttrEnum.PARTED
    assert b9(t) == h2b(
        "0x010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000"
    )

    # keyed table
    # q)-8!([a:enlist 2i]b:enlist 3i)
    # dictionary with tables for keys and values
    key_hdr = ktn(TypeEnum.KS).appendS("a")
    key_val = ktn(TypeEnum.K)
    key_val.kK().append(ktn(TypeEnum.KI))
    key_val.kK()[0].kI().append(2)

    val_hdr = ktn(TypeEnum.KS).appendS("b")
    val_val = ktn(TypeEnum.K)
    val_val.kK().append(ktn(TypeEnum.KI))
    val_val.kK()[0].kI().append(3)

    kt = xd(xt(xd(key_hdr, key_val)), xt(xd(val_hdr, val_val)))
    assert b9(kt) == h2b(
        "0x010000003f000000636200630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000"
    )

    # sorted keyed table
    # q)-8!`s#([a:enlist 2i]b:enlist 3i)
    # outer dict is sorted dict (7f) with sorted table (62) as key, no parted bit set
    kt = xd(
        xt(xd(key_hdr, key_val), sorted=True), xt(xd(val_hdr, val_val)), sorted=True
    )
    assert (
        b2h(b9(kt))
        == "0x010000003f0000007f6201630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000"
    )
