import struct
from typing import Self
from collections.abc import MutableSequence, Sequence
import enum
import array

__all__ = [
    "b9",
    "d9",
    "ktn",
]


class NotImplementedException(Exception):
    pass


class AttrEnum(enum.IntEnum):
    NONE = 0
    SORTED = 1


class TypeEnum(enum.IntEnum):
    #    type bytes qtype     ctype  accessor
    K = 0  #   *    K         K
    KB = 1  #  1    boolean   char   kG
    UU = 2  #  16   guid      U      kU
    KG = 4  #  1    byte      char   kG
    KH = 5  #  2    short     short  kH
    KI = 6  #  4    int       int    kI
    KJ = 7  #  8    long      long   kJ
    KE = 8  #  4    real      float  kE
    KF = 9  #  8    float     double kF
    KC = 10  # 1    char      char   kC
    KS = 11  # *    symbol    char*  kS
    KP = 12  # 8    timestamp long   kJ (nanoseconds from 2000.01.01)
    KM = 13  # 4    month     int    kI (months from 2000.01.01)
    KD = 14  # 4    date      int    kI (days from 2000.01.01)
    KN = 16  # 8    timespan  long   kJ (nanoseconds)
    KU = 17  # 4    minute    int    kI
    KV = 18  # 4    second    int    kI
    KT = 19  # 4    time      int    kI (millisecond)
    KZ = 15  # 8    datetime  double kF (DO NOT USE)
    XT = 98  # table
    XD = 99  # dict
    SD = 127  # sorted dict
    KRR = -128  # error


class KContext:
    def __init__(self) -> None:
        self.symbols: dict[str, int] = {}
        self.symbols_enc: dict[int, tuple[str, bytes]] = {}

    def ss(self, s: str):
        # we don't want any surprises trying to serialise non-ascii symbols later
        # so force non-ascii characters out now
        bs = bytes(s, "ascii") + b"\x00"
        idx = self.symbols.setdefault(s, len(self.symbols))
        # TODO this should be a list
        self.symbols_enc[idx] = (s, bs)
        return idx


DEFAULT_CONTEXT = KContext()


def tn(t: int) -> str:
    return f"{TypeEnum(abs(t)).name} ({t})"


class KObj:
    def __init__(self, t: int = 0, context: KContext = DEFAULT_CONTEXT, attr: int = 0):
        self.t = t
        self.attrib = attr
        self.context: KContext = context

    def _paysz(self):
        return 1

    def _databytes(self) -> bytes:
        return struct.pack("<b", self.t)

    def _tn(self) -> str:
        return tn(self.t)

    # atom getters
    def aI(self) -> int:
        raise NotImplementedException

    def aH(self) -> int:
        raise NotImplementedException

    def aJ(self) -> int:
        raise NotImplementedException

    def aS(self) -> str:
        raise NotImplementedException

    # vector getters
    def kI(self) -> MutableSequence[int]:
        raise NotImplementedException

    def kS(self) -> Sequence[str]:
        raise NotImplementedException


class KObjAtom(KObj):
    def __init__(self, t: int = 0, context: KContext = DEFAULT_CONTEXT):
        super().__init__(t, context)
        self.data: bytes = b""

    # atom setters
    def i(self, i: int) -> Self:
        if self.t not in [-TypeEnum.KI]:
            raise ValueError(f"wrong type {self._tn()} for i()")
        self.data = struct.pack("i", i)
        return self

    def j(self, j: int) -> Self:
        if self.t not in [-TypeEnum.KJ]:
            raise ValueError(f"wrong type {self._tn()} for j()")
        self.data = struct.pack("q", j)
        return self

    def ss(self, s: str) -> Self:
        if self.t not in [-TypeEnum.KS]:
            raise ValueError(f"wrong type {self._tn()} for ss()")
        self.data = struct.pack("i", self.context.ss(s))
        return self

    # atom getters
    def aH(self) -> int:
        if self.t not in [-TypeEnum.KH]:
            raise ValueError(f"wrong type {self.t} for kH")
        return struct.unpack("h", self.data)[0]

    def aI(self) -> int:
        if self.t not in [-TypeEnum.KI, -TypeEnum.KS]:
            raise ValueError(f"wrong type {self.t} for kI")
        return struct.unpack("i", self.data)[0]

    def aJ(self) -> int:
        if self.t not in [-TypeEnum.KJ]:
            raise ValueError(f"wrong type {self.t} for kJ")
        return struct.unpack("q", self.data)[0]

    def aS(self) -> str:
        return self.context.symbols_enc[self.aI()][0]

    def _databytes(self):
        bs = self.data
        if self.t == -TypeEnum.KS:
            bs = self.context.symbols_enc[self.aI()][1]

        return super()._databytes() + bs

    def _paysz(self):
        sz = len(self.data)
        if self.t == -TypeEnum.KS:
            sz = len(self.context.symbols_enc[self.aI()][1])
        return super()._paysz() + len(self.data)


class KIntArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KI, attr: int = 0):
        super().__init__(t, attr=attr)
        self.j = array.array("l", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 4 * len(self.j)

    def _databytes(self):
        if self.j.itemsize == 4:  # array type 'l' seems to store as 8 bytes
            pi = self.j.tobytes()
        else:
            pi = struct.pack(f"<{len(self.j)}I", *self.j)
        return struct.pack("<bBI", self.t, self.attrib, len(self.j)) + pi

    def kI(self) -> MutableSequence[int]:
        return self.j


class KIntSymArray(KIntArray):
    # store symbol indexes in KIntArray
    # hook serialise to use null byte terminated representation
    def _paysz(self):
        return 2 + 4 + sum([len(self.context.symbols_enc[j][1]) for j in self.j])

    def _databytes(self):
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self.j))]
        for j in self.j:
            parts.append(self.context.symbols_enc[j][1])
        return b"".join(parts)

    def kS(self) -> Sequence[str]:
        # Warning: accessor read-only
        s = []
        for j in self.j:
            s.append(self.context.symbols_enc[j][0])
        return s

    def appendS(self, *ss: str) -> Self:
        for s in ss:
            j = self.context.ss(s)
            self.j.append(j)
        return self


class KByteArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KB, attr: int = 0):
        super().__init__(t, attr=attr)
        self.g = array.array("b", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 1 * len(self.g)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self.g)) + struct.pack(
            f"<{len(self.g)}B", *self.g
        )

    def kB(self) -> MutableSequence[int]:
        return self.g


class KLongArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KJ, attr: int = 0):
        super().__init__(t, attr=attr)
        self.j = array.array("q", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 8 * len(self.j)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self.j)) + struct.pack(
            f"<{len(self.j)}q", *self.j
        )

    def kI(self) -> MutableSequence[int]:
        return self.j


class KListArray(KObj):
    def __init__(self):
        super().__init__(0)
        self.k = []

    def _paysz(self):
        # sum sizes nested ks
        return 2 + 4 + sum([ko._paysz() for ko in self.k])

    def _databytes(self):
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self.k))]
        parts.extend(ko._databytes() for ko in self.k)
        return b"".join(parts)


def b9(k: KObj, msgtype=0, flags=0) -> bytes:
    # 8 byte header
    msglen = 8 + k._paysz()
    return struct.pack("<BBHI", 1, msgtype, flags, msglen) + k._databytes()


# atom constructors
def ki(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KI).i(i)


def kj(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KJ).j(i)


def ks(s: str) -> KObj:
    return KObjAtom(-TypeEnum.KS).ss(s)


# vector constructors
def ktn(t: int, sz: int = 0, sorted: bool = False) -> KObj:
    attr = AttrEnum.NONE
    if sorted:
        attr = AttrEnum.SORTED

    if t == TypeEnum.KI:
        return KIntArray(sz, t, attr)
    if t == TypeEnum.KG:
        return KByteArray(sz, t, attr)
    if t == TypeEnum.KJ:
        return KLongArray(sz, t, attr)
    if t == TypeEnum.KS:
        return KIntSymArray(sz, t, attr)
    if t == 0:
        return KListArray()

    raise NotImplementedException(f"ktn for type {tn(t)}")


class KDict(KObj):
    def __init__(self, kkeys: KObj, kvalues: KObj, t: TypeEnum = TypeEnum.XD):
        super().__init__(t)
        if t == TypeEnum.SD and kkeys.attrib == 0:
            raise ValueError(f"Keys not sorted for SD {kkeys._tn()}")
        self.kkeys = kkeys
        self.kvalues = kvalues

    def _paysz(self):
        return 1 + self.kkeys._paysz() + self.kvalues._paysz()

    def _databytes(self):
        return (
            struct.pack("<B", self.t)
            + self.kkeys._databytes()
            + self.kvalues._databytes()
        )


def xd(kkeys: KObj, kvalues: KObj, sorted=False) -> KObj:
    if sorted:
        return KDict(kkeys, kvalues, TypeEnum.SD)
    return KDict(kkeys, kvalues)
