import itertools
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from aiokdb import KObj, TypeEnum

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
        self.maxchars = width * height

    def format(self, obj: KObj) -> str:
        if obj.t == TypeEnum.XT:
            return self._fmt_unkeyed_table(obj)
        elif obj.t == TypeEnum.XD and obj.kkey().t == TypeEnum.XT:
            return self._fmt_keyed_table(obj)
        return repr(obj)

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

    def _format_table_rows(self, obj: KObj, rows: Iterable[Optional[int]]) -> list[str]:
        d1 = obj.kvalue()

        colNames: Sequence[str] = d1.kkey().kS()
        kv = d1.kvalue().kK()

        colWidths: list[int] = list(map(len, colNames))

        # stringify all cells within our rowiter
        rowSample: list[list[str]] = []
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

        if obj.t == TypeEnum.KJ:
            j = obj.kJ()[index]
            if j == -9223372036854775808:
                return ""
            if j == 9223372036854775807:
                return "0W"
            return str(j)
        elif obj.t == TypeEnum.KI:
            i = obj.kI()[index]
            if i == -2147483648:
                return ""
            return str(i)
        elif obj.t == TypeEnum.KS:
            return obj.kS()[index]
        elif obj.t == TypeEnum.KG or obj.t == TypeEnum.KB:
            return str(obj.kG()[index])
        elif obj.t == TypeEnum.KN:
            j = obj.kJ()[index]
            if j == -9223372036854775808:
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
        elif obj.t == TypeEnum.K:
            # how much of obj should we show?
            return "KObj?"
        elif obj.t == TypeEnum.UU:
            return str(obj.kU()[index])
        elif obj.t == TypeEnum.KF:
            return str(obj.kF()[index])
        elif obj.t == TypeEnum.KP:
            j = obj.kJ()[index]
            if j == -9223372036854775808:
                return ""
            # timestamp (nanos) q)"p"$1  2000.01.01D00:00:00.000000001
            # python timestamps have microsecond precision, so on our own
            # formatting with full precision
            nanos = j % 1000
            micros = j // 1000
            origin = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
            dt = datetime.utcfromtimestamp(origin + micros / 1000000.0)
            return dt.strftime("%Y.%m.%dD%H:%M:%S:%f") + f"{nanos:03}"
        raise ValueError(f"No formatter for {obj} with type {obj._tn()}")
