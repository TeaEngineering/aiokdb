import uuid
from typing import Callable, Optional

from aiokdb import KObj, TypeEnum, cv, d9, ka, kj, kk, ks, ktn, xd, xt
from aiokdb.extras import ktni, ktns
from aiokdb.format import AsciiFormatter, HtmlFormatter


def test_format_unkeyed_table() -> None:
    fmt = AsciiFormatter(height=8)

    # q)-8!([]a:enlist 2i;b:enlist 3i)
    ks = ktns("a", "b")
    kv = kk(ktni(TypeEnum.KI, 2), ktni(TypeEnum.KI, 3))
    t = xt(xd(ks, kv))

    assert fmt.format(t) == "a b\n---\n2 3"

    # first col pushed out by col name, 2nd by value width
    ks = ktns("alpha", "b", "cheese")
    kv = kk(
        ktni(TypeEnum.KI, 0, 1, 2),
        ktni(TypeEnum.KI, 0, 17, 0),
        ktni(TypeEnum.KI, 0, 0, 0),
    )
    t = xt(xd(ks, kv))

    expected = "alpha b  cheese\n---------------\n0     0  0     \n1     17 0     \n2     0  0     "
    assert fmt.format(t) == expected

    # introducing the ... fold at halfway
    ks = ktns("long")
    kv = kk(ktn(TypeEnum.KI, 50))
    kv.kK()[0].kI()[49] = 49
    t = xt(xd(ks, kv))
    expected = "long\n----\n0   \n0   \n... \n0   \n49  "
    assert fmt.format(t) == expected


def test_format_unkeyed_html() -> None:
    fmt = HtmlFormatter(table_class="table table-striped table-condensed", indent=2)
    # q)-8!([]a:2 1i;b:3 4i)
    ks = ktns("a", "b")
    kv = kk(ktni(TypeEnum.KI, 2, 1), ktni(TypeEnum.KI, 3, 4))
    t = xt(xd(ks, kv))
    assert (
        fmt.format(t)
        == """
<table class="table table-striped table-condensed">
  <thead>
    <tr>
      <th>a</th>
      <th>b</th>
    </tr>
  </thead>
  <tr>
    <td>2</td>
    <td>3</td>
  </tr>
  <tr>
    <td>1</td>
    <td>4</td>
  </tr>
</table>""".strip()
    )


def test_format_unkeyed_escape() -> None:
    fmt = HtmlFormatter(indent=2)
    # q)-8!([]a:2 1i;b:3 4i)
    ks = ktns("a", "b")
    kv = kk(ktni(TypeEnum.KI, 2, 1), ktns("hi", "<script>alert(1)</script>"))
    t = xt(xd(ks, kv))
    assert (
        fmt.format(t)
        == """
<table>
  <thead>
    <tr>
      <th>a</th>
      <th>b</th>
    </tr>
  </thead>
  <tr>
    <td>2</td>
    <td>hi</td>
  </tr>
  <tr>
    <td>1</td>
    <td>&lt;script&gt;alert(1)&lt;/script&gt;</td>
  </tr>
</table>""".strip()
    )


def test_format_keyed_table() -> None:
    expected = "a| b\n-|--\n2| 3"

    t = d9(
        bytes.fromhex(
            "010000003f000000636200630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000"
        )
    )
    fmt = AsciiFormatter()
    assert fmt.format(t) == expected


def test_format_keyed_table_html() -> None:
    t = d9(
        bytes.fromhex(
            "010000003f000000636200630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000"
        )
    )
    fmt = HtmlFormatter(table_class="table")
    assert (
        fmt.format(t)
        == """
<table class="table">
  <thead>
    <tr>
      <th>a</th>
      <th>b</th>
    </tr>
  </thead>
  <tr>
    <th>2</th>
    <td>3</td>
  </tr>
</table>""".strip()
    )

    ## certain named column in bold
    class SillyHtmlFormatter(HtmlFormatter):
        def get_table_cell_formatter_for(
            self, kob: KObj, isKey: bool, i: int, colName: str
        ) -> Callable[[KObj, int, Optional[int]], str]:
            if colName == "b":
                return self.bold_html_cell
            return super().get_table_cell_formatter_for(kob, isKey, i, colName)

        def bold_html_cell(self, obj: KObj, col: int, index: Optional[int]) -> str:
            return self.markup(
                "<b>" + self.escape(self._str_cell(obj, col, index)) + "</b>"
            )

    b = SillyHtmlFormatter()
    assert (
        b.format(t)
        == """
<table>
  <thead>
    <tr>
      <th>a</th>
      <th>b</th>
    </tr>
  </thead>
  <tr>
    <th>2</th>
    <td><b>3</b></td>
  </tr>
</table>""".strip()
    )


def test_format_keyed_table_nested_str() -> None:
    expected = "envelope_id                         | payload time                         \n------------------------------------|--------------------------------------\n2d948578-e9d6-79a2-8207-9df7a71f0b3b| abc     2024.05.14D23:13:19:044908000\n409031f3-b19c-6770-ee84-6e9369c98697| xy      2024.05.14D23:13:19:044908000"
    # keyed table
    # dictionary with tables for keys and values
    key_hdr = ktns("envelope_id")
    key_val = kk(ktn(TypeEnum.UU))

    val_hdr = ktns("payload", "time")
    val_val = kk(ktn(TypeEnum.K), ktn(TypeEnum.KP))
    kt = xd(xt(xd(key_hdr, key_val)), xt(xd(val_hdr, val_val)))

    kt.kkey()["envelope_id"].kU().extend(
        [
            uuid.UUID("2d948578-e9d6-79a2-8207-9df7a71f0b3b"),
            uuid.UUID("409031f3-b19c-6770-ee84-6e9369c98697"),
        ]
    )
    kt.kvalue()["payload"].kK().extend([cv("abc"), cv("xy")])
    kt.kvalue()["time"].kJ().extend([769043599044908000, 769043599044908000])

    assert AsciiFormatter().format(kt) == expected


def test_format_dict() -> None:
    d = xd(ktni(TypeEnum.KJ, 3, 612, 6), ktns("hi", "p", "dog"))
    fmt = AsciiFormatter()
    assert fmt.format(d) == "3  | hi\n612| p\n6  | dog"

    d = xd(ktni(TypeEnum.KJ, 3, 612, 6), kk(kj(56), ks("xray"), ka(-TypeEnum.UU)))
    assert (
        fmt.format(d) == "3  | 56\n612| xray\n6  | 00000000-0000-0000-0000-000000000000"
    )

    assert fmt.format(kj(-9223372036854775807)) == "-0W"

    ht = HtmlFormatter()
    assert (
        ht.format(d)
        == """
<dl>
  <dt>3</dt>
  <dd>56</dd>
  <dt>612</dt>
  <dd>xray</dd>
  <dt>6</dt>
  <dd>00000000-0000-0000-0000-000000000000</dd>
</dl>
""".strip()
    )


def test_format_atoms() -> None:
    fmt = AsciiFormatter(height=8)

    p = ka(-TypeEnum.KP)
    p.j(1000000000)
    assert fmt.format(p) == "2000.01.01D00:00:01:000000000"

    # square brackets to show the mixed-type nature of K-arrays
    p = kk(kj(5), ks("hello"))
    assert fmt.format(p) == "[5, hello]"

    p = ktni(TypeEnum.KJ, *range(20))
    assert fmt.format(p) == "0 1 ... 18 19"
