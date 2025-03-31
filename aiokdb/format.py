import itertools
from datetime import datetime, timezone
from html import escape
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from aiokdb import KObj, Nulls, TypeEnum

# Keeping formating external to KObj since it complicates the implementation.
# This produces AsciiFormatter produces numpy-like formatting, ie. it truncates the central section of a table
#
#    a b| addr e  f
#    ---|----------
#    2 1| 1    44 7
#    ...|...
#    3 4| abc  34 4
#
# allows customisation by subclassing
# TODO: truncate width


class AsciiFormatter:
    def __init__(self, width: int = 200, height: int = 10):
        self.width = width
        self.height = height
        self.inline_chars = width / 3

    def format(self, obj: KObj) -> str:
        if obj.t == TypeEnum.XT:
            return self._fmt_unkeyed_table(obj)
        elif obj.t == TypeEnum.XD and obj.kkey().t == TypeEnum.XT:
            return self._fmt_keyed_table(obj)
        elif obj.t == TypeEnum.XD:
            return self._fmt_dict(obj)
        return self._fmt_inline(obj)

    def _fmt_unkeyed_table(self, obj: KObj) -> str:
        rowcount = self._table_conforms(obj)
        rows = self._select_rows(rowcount)
        return "\n".join(self._format_table_rows(obj, rows))

    def _select_rows(self, rowcount: int) -> Iterable[Optional[int]]:
        if rowcount < self.height - 2:
            # no need to truncate rows
            rows: Iterable[Optional[int]] = itertools.chain(range(rowcount))
        else:
            chunk = (self.height - 3) // 2
            rows = itertools.chain(
                range(chunk), [None], range(rowcount - chunk, rowcount)
            )
        return rows

    def _table_conforms(self, obj: KObj) -> int:
        assert obj.t == TypeEnum.XT
        obj = obj.kvalue()
        assert obj.t == TypeEnum.XD
        assert obj.kkey().t == TypeEnum.KS
        assert obj.kvalue().t == TypeEnum.K

        # check rows conform
        kv = obj.kvalue().kK()
        rowcount = len(kv[0])
        for i in range(len(obj.kkey())):
            assert len(kv[i]) == rowcount
        return rowcount

    def _fmt_keyed_table(self, obj: KObj) -> str:
        ktv = obj.kvalue()
        ktk = obj.kkey()

        # check rows conform
        keyrowcount = self._table_conforms(ktv)
        valuerowcount = self._table_conforms(ktk)

        assert keyrowcount == valuerowcount
        rows = list(self._select_rows(keyrowcount))

        left = self._format_table_rows(obj.kkey(), rows)
        right = self._format_table_rows(obj.kvalue(), rows)

        filler = [" ", "-"] + [" "] * len(rows)
        return "\n".join(f"{ll}|{g}{rr}" for g, ll, rr in zip(filler, left, right))

    def _format_table_rows(self, obj: KObj, rows: Iterable[Optional[int]]) -> List[str]:
        d1 = obj.kvalue()

        colNames: Sequence[str] = d1.kkey().kS()
        kv = d1.kvalue().kK()

        colWidths: List[int] = list(map(len, colNames))

        # stringify all cells within our rowiter
        rowSample: List[List[str]] = []
        for r in rows:
            cs = []
            for c in range(len(colNames)):
                s = self._str_cell(kv[c], c, r)
                colWidths[c] = max(colWidths[c], len(s))
                cs.append(s)
            rowSample.append(cs)

        # now do the padding of pre-stringified cells
        rowText = []
        for row in rowSample:
            rowText.append(" ".join([f"{t:{w}}" for w, t in zip(colWidths, row)]))

        headers = " ".join([f"{t:{w}}" for w, t in zip(colWidths, colNames)])
        dashes = "".join(["-"] * len(headers))
        return [headers, dashes, *rowText]

    def _str_cell(self, obj: KObj, col: int, index: Optional[int]) -> str:
        if index is None:
            if col == 0:
                return "..."
            return ""

        if obj.t == TypeEnum.KG or obj.t == TypeEnum.KB:
            return str(obj.kG()[index])
        elif obj.t == TypeEnum.KH:
            i = obj.kH()[index]
            if i == Nulls.h:
                return ""
            return str(i)
        elif obj.t == TypeEnum.KI:
            i = obj.kI()[index]
            if i == Nulls.i:
                return ""
            return str(i)
        elif obj.t == TypeEnum.KJ:
            return self._fmt_atom_j(obj.kJ()[index])
        elif obj.t == TypeEnum.KM:
            return self._fmt_atom_m(obj.kI()[index])
        elif obj.t == TypeEnum.KD:
            return self._fmt_atom_d(obj.kI()[index])
        elif obj.t == TypeEnum.KZ:
            return self._fmt_atom_z(obj.kF()[index])
        elif obj.t == TypeEnum.KU:
            return self._fmt_atom_u(obj.kI()[index])
        elif obj.t == TypeEnum.KV:
            return self._fmt_atom_v(obj.kI()[index])
        elif obj.t == TypeEnum.KT:
            return self._fmt_atom_t(obj.kI()[index])
        elif obj.t == TypeEnum.KS:
            return obj.kS()[index]
        elif obj.t == TypeEnum.KN:
            j = obj.kJ()[index]
            return self._fmt_atom_n(j)
        elif obj.t == TypeEnum.K:
            # how much of obj should we show?
            o = obj.kK()[index]
            return self._fmt_inline(o)
        elif obj.t == TypeEnum.UU:
            return str(obj.kU()[index])
        elif obj.t == TypeEnum.KE:
            e = obj.kE()[index]
            return str(e)
        elif obj.t == TypeEnum.KF:
            return str(obj.kF()[index])
        elif obj.t == TypeEnum.KP:
            j = obj.kJ()[index]
            return self._fmt_atom_p(j)
        elif obj.t == TypeEnum.KC:
            return obj.aS()[index]
        raise ValueError(f"No cell formatter for {obj} with type {obj._tn()}")

    def _fmt_atom_j(self, j: int) -> str:
        if j == Nulls.j:
            return ""
        elif j == 9223372036854775807:
            return "0W"
        elif j == -9223372036854775807:
            return "-0W"
        return str(j)

    def _fmt_atom_p(self, j: int) -> str:
        if j == Nulls.j:
            return ""
        # timestamp (nanos) q)"p"$1  2000.01.01D00:00:00.000000001
        # python timestamps have microsecond precision, so on our own
        # formatting with full precision
        nanos = j % 1000
        micros = j // 1000
        origin = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
        dt = datetime.utcfromtimestamp(origin + micros / 1000000.0)
        return dt.strftime("%Y.%m.%dD%H:%M:%S:%f") + f"{nanos:03}"

    def _fmt_atom_n(self, j: int) -> str:
        if j == Nulls.j:
            return ""
        # timespan (nanos) q) "n"$1  0D00:00:00.000000001
        secs = j // 1000000000
        m = secs // 60
        h = m // 60
        d = h // 24
        nanos = j % 1000000000
        secs = secs % 60
        m = m % 60
        h = h % 24
        return f"{d}D{h:02}:{m:02}:{secs:02}.{nanos:09}"

    def _fmt_atom_m(self, m: int) -> str:
        if m == Nulls.i:
            return "0Nm"
        return f"{m / 12:04}.{m % 12:02}m"

    def _fmt_atom_d(self, d: int) -> str:
        if d == Nulls.i:
            return "0Nd"
        origin = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
        dt = datetime.utcfromtimestamp(origin + d)
        return dt.strftime("%Y.%m.%d")

    def _fmt_atom_z(self, d: float) -> str:
        if d == Nulls.f:
            return "0Nz"
        origin = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
        dt = datetime.utcfromtimestamp(origin + d)
        return dt.strftime("%Y.%m.%dT%H:%M:%S:%f")

    def _fmt_atom_u(self, u: int) -> str:
        if u == Nulls.i:
            return "0Nu"
        # TODO: formatting >1h etc
        return f"{u / 60:02}:{u % 60:02}"

    def _fmt_atom_v(self, v: int) -> str:
        if v == Nulls.i:
            return "0Nv"
        h = v / 3600
        m = (v / 60) % 60
        s = v % 60
        return f"{h:02}:{m:02}:{s:02}"

    def _fmt_atom_t(self, t: int) -> str:
        if t == Nulls.i:
            return "ONt"
        # timespan in nanos
        secs = t // 1000000000
        m = secs // 60
        h = m // 60
        d = h // 24
        nanos = t % 1000000000
        secs = secs % 60
        m = m % 60
        h = h % 24
        return f"{d}D{h:02}:{m:02}:{secs:02}.{nanos:09}"

    def _fmt_dict(self, obj: KObj) -> str:
        # measure the keys
        rows = list(self._select_rows(len(obj)))
        ks, vs = [], []
        keywidth = 0
        for r in rows:
            k = self._str_cell(obj.kkey(), 0, r)
            v = self._str_cell(obj.kvalue(), 0, r)
            keywidth = max(keywidth, len(k))
            ks.append(k)
            vs.append(v)

        return "\n".join([f"{k:{keywidth}}| {v}" for k, v in zip(ks, vs)])

    def _fmt_inline(self, obj: KObj) -> str:
        # we are expecting to format the object in a constrained environment, ie. within the
        # cell of a table or dictionary. We can take up at most one line of text and width
        # of inline_chars
        if obj.t == -TypeEnum.KJ:
            j = obj.aJ()
            return self._fmt_atom_j(j)
        elif obj.t == -TypeEnum.KI:
            i = obj.aI()
            if i == Nulls.i:
                return ""
            return str(i) + "i"
        elif obj.t == -TypeEnum.KH:
            i = obj.aH()
            return str(i) + "h"
        elif obj.t == -TypeEnum.KE:
            e = obj.aE()
            return str(e) + "e"
        elif obj.t == -TypeEnum.KS:
            return obj.aS()
        elif obj.t == -TypeEnum.KC:
            return f'"{obj.aC()}"'
        elif obj.t == -TypeEnum.KG or obj.t == -TypeEnum.KB:
            return str(obj.aG())
        elif obj.t == -TypeEnum.KN:
            return self._fmt_atom_n(obj.aJ())
        elif obj.t == -TypeEnum.KM:
            return self._fmt_atom_m(obj.aI())
        elif obj.t == -TypeEnum.KD:
            return self._fmt_atom_d(obj.aI())
        elif obj.t == -TypeEnum.KZ:
            return self._fmt_atom_z(obj.aF())
        elif obj.t == -TypeEnum.KU:
            return self._fmt_atom_u(obj.aI())
        elif obj.t == -TypeEnum.KV:
            return self._fmt_atom_v(obj.aI())
        elif obj.t == -TypeEnum.KT:
            return self._fmt_atom_t(obj.aI())
        elif obj.t == -TypeEnum.UU:
            return str(obj.aU())
        elif obj.t == -TypeEnum.KF:
            return str(obj.aF())
        elif obj.t == -TypeEnum.KP:
            return self._fmt_atom_p(obj.aJ())

        # vectors
        elif obj.t == TypeEnum.K:
            # sample the vector (first five?)
            elems = list(self._select_rows(len(obj)))
            ks = ", ".join([self._str_cell(obj, 0, r) for r in elems])
            return f"[{ks}]"
        elif obj.t == TypeEnum.KC:
            elems = list(self._select_rows(len(obj)))
            return "".join([self._str_cell(obj, 0, r) for r in elems])

        elif obj.t > 0 and obj.t < 20:
            # sample the vector (first five?)
            elems = list(self._select_rows(len(obj)))
            ks = " ".join([self._str_cell(obj, 0, r) for r in elems])
            return ks

        elif obj.t in (TypeEnum.XD, TypeEnum.SD):
            return "KDict"
        elif obj.t == TypeEnum.XT:
            return "KTable"
        elif obj.t == TypeEnum.NIL:
            return "::"
        elif obj.t == TypeEnum.FN:
            return obj.aS()

        raise ValueError(f"No inline formatter for {obj} with type {obj._tn()}")


def identity(x: str) -> str:
    return x


class HtmlFormatter(AsciiFormatter):
    def __init__(
        self,
        table_class: Optional[str] = None,
        indent: int = 2,
        width: int = 200,
        height: int = 10,
        markup: Callable[[str], str] = identity,
        escape: Callable[[str], str] = escape,
    ):
        super().__init__(width, height)
        self.tc = f' class="{table_class}"' if table_class else ""
        self.indent = indent
        self.markup = markup
        self.escape = escape

    def format(self, obj: KObj) -> str:
        if obj.t == TypeEnum.XT:
            return self._fmt_unkeyed_table(obj)
        elif obj.t == TypeEnum.XD and obj.kkey().t == TypeEnum.XT:
            return self._fmt_keyed_table(obj)
        elif obj.t == TypeEnum.XD:
            return self._fmt_dict(obj)
        return self.escape(self._fmt_inline(obj))

    def _fmt_unkeyed_table(self, obj: KObj) -> str:
        rowcount = self._table_conforms(obj)
        rows = self._select_rows(rowcount)

        d1 = obj.kvalue()

        colNames: Sequence[str] = d1.kkey().kS()
        table_col_formatters = [
            self.get_table_cell_formatter_for(obj, False, c, cn)
            for c, cn in enumerate(colNames)
        ]

        kv = d1.kvalue().kK()

        # stringify all cells within our rowiter
        rowSample: List[List[str]] = []
        for r in rows:
            cs = []
            for c, cn in enumerate(colNames):
                st = table_col_formatters[c](kv[c], c, r)
                cs.append(st)
            rowSample.append(cs)

        rowHtml = "\n".join(
            [
                f"<table{self.tc}>",
                "  <thead>",
                "    <tr>",
                "\n".join(f"      <th>{r}</th>" for r in colNames),
                "    </tr>",
                "  </thead>",
                "\n".join(
                    [
                        "  <tr>\n"
                        + "\n".join([f"    <td>{c}</td>" for c in row])
                        + "\n  </tr>"
                        for row in rowSample
                    ]
                ),
                "</table>",
            ]
        )
        return self.markup(rowHtml)

    def _html_cell(self, obj: KObj, col: int, index: Optional[int]) -> str:
        return self.escape(self._str_cell(obj, col, index))

    def get_table_cell_formatter_for(
        self, kob: KObj, isKey: bool, i: int, colName: str
    ) -> Callable[[KObj, int, Optional[int]], str]:
        return self._html_cell

    def _fmt_keyed_table(self, obj: KObj) -> str:
        ktk = obj.kkey()
        ktv = obj.kvalue()

        # check rows conform
        keyrowcount = self._table_conforms(ktv)
        valuerowcount = self._table_conforms(ktk)

        assert keyrowcount == valuerowcount
        rows = list(self._select_rows(keyrowcount))

        # TODO: move get_table_cell_formatter_for lookup here
        colMeta: List[Tuple[KObj, bool, int, str]] = [
            (obj, True, i, s) for i, s in enumerate(ktk.kS())
        ] + [(obj, False, i, s) for i, s in enumerate(ktv.kS())]

        table_col_formatters = [self.get_table_cell_formatter_for(*x) for x in colMeta]
        rowSample: List[List[Tuple[str, bool]]] = []
        for r in rows:
            cs = []
            for c, (kobj, iskey, i, s) in enumerate(colMeta):
                ktable = ktk if iskey else ktv
                columnv = ktable.kvalue().kvalue().kK()[i]
                st = table_col_formatters[c](columnv, c, r)
                cs.append((st, iskey))
            rowSample.append(cs)

        w = {True: "th", False: "td"}

        rowHtml = "\n".join(
            [
                f"<table{self.tc}>",
                "  <thead>",
                "    <tr>",
                "\n".join(f"      <th>{s}</th>" for kobj, ik, i, s in colMeta),
                "    </tr>",
                "  </thead>",
                "\n".join(
                    [
                        "  <tr>\n"
                        + "\n".join([f"    <{w[k]}>{c}</{w[k]}>" for c, k in row])
                        + "\n  </tr>"
                        for row in rowSample
                    ]
                ),
                "</table>",
            ]
        )
        return self.markup(rowHtml)

    def _fmt_dict(self, obj: KObj) -> str:
        rows = list(self._select_rows(len(obj)))
        ks, vs = [], []
        for r in rows:
            k = self.escape(self._str_cell(obj.kkey(), 0, r))
            v = self.escape(self._str_cell(obj.kvalue(), 0, r))
            ks.append(k)
            vs.append(v)

        return self.markup(
            "".join(
                [
                    "<dl>\n",
                    "\n".join(
                        [f"  <dt>{k}</dt>\n  <dd>{v}</dd>" for k, v in zip(ks, vs)]
                    ),
                    "\n</dl>",
                ]
            )
        )
