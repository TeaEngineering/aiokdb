from aiokdb import TypeEnum, d9, ktn, xd, xt
from aiokdb.format import AsciiFormatter


def test_format_unkeyed_table() -> None:
    fmt = AsciiFormatter(height=8)

    # q)-8!([]a:enlist 2i;b:enlist 3i)
    ks = ktn(TypeEnum.KS).appendS("a", "b")
    kv = ktn(TypeEnum.K)
    kv.kK().extend([ktn(TypeEnum.KI, 1), ktn(TypeEnum.KI, 1)])
    kv.kK()[0].kI()[0] = 2
    kv.kK()[1].kI()[0] = 3
    t = xt(xd(ks, kv))

    assert fmt.format(t) == "a b\n---\n2 3"

    # first col pushed out by col name, 2nd by value width
    ks = ktn(TypeEnum.KS).appendS("alpha", "b", "cheese")
    kv = ktn(TypeEnum.K)
    kv.kK().extend([ktn(TypeEnum.KI, 3), ktn(TypeEnum.KI, 3), ktn(TypeEnum.KI, 3)])
    kv.kK()[0].kI()[1] = 1
    kv.kK()[0].kI()[2] = 2
    kv.kK()[1].kI()[1] = 17
    t = xt(xd(ks, kv))

    expected = "alpha b  cheese\n---------------\n0     0  0     \n1     17 0     \n2     0  0     "
    assert fmt.format(t) == expected

    # introducing the ... fold at halfway
    ks = ktn(TypeEnum.KS).appendS("long")
    kv = ktn(TypeEnum.K)
    kv.kK().extend([ktn(TypeEnum.KI, 50)])
    kv.kK()[0].kI()[49] = 49
    t = xt(xd(ks, kv))
    expected = "long\n----\n0   \n0   \n... \n0   \n49  "
    assert fmt.format(t) == expected


def test_format_keyed_table() -> None:
    expected = """\
a| b
-|--
2| 3"""

    t = d9(
        bytes.fromhex(
            "010000003f000000636200630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000"
        )
    )
    fmt = AsciiFormatter()
    assert fmt.format(t) == expected
