import struct
from typing import Self
import enum
import array

__all__ = [
    "b9",
    "d9",
    "ktn",
]

class NotImplementedException(Exception):
    pass

class TypeEnum(enum.IntEnum):
    #    type bytes qtype     ctype  accessor
    KB = 1  #  1    boolean   char   kG
    UU = 2  #  16   guid     U       kU
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
    SD = 127 # sorted dict
    KRR = -128 # error

class KContext:
    def __init__(self):
        self.symbols: Dict[str,int] = {}
        self.symbols_enc: Dict[int,bytes] = {}
    def ss(self, s: str):
        # we don't want any surprises trying to serialise non-ascii symbols later
        # so force non-ascii characters out now
        bs = bytes(s, 'ascii')
        idx = self.symbols.setdefault(s, len(self.symbols))
        # TODO this should be a list
        self.symbols_enc[idx] = bs
        return idx

DEFAULT_CONTEXT = KContext()

class KObj:
    def __init__(self, t: int = 0, context: KContext=DEFAULT_CONTEXT):
        self.t = t
        self.context: KContext = context
    def _paysz(self):
        return 0

    def _databytes(self) -> bytes:
        return b""


class KObjAtom(KObj):
    def __init__(self, t: int = 0, context: KContext=DEFAULT_CONTEXT):
        super().__init__(t, context)
        self.data: bytes = b""

    def _tn(self) -> str:
        return f"{TypeEnum(abs(self.t)).name} ({self.t})"

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
    def kH(self) -> int:
        if self.t not in [-TypeEnum.KH]:
            raise ValueError(f"wrong type {self.t} for kH")
        return struct.unpack("h", self.data)[0]

    def kI(self) -> int:
        if self.t not in [-TypeEnum.KI]:
            raise ValueError(f"wrong type {self.t} for kI")
        return struct.unpack("i", self.data)[0]

    def kJ(self) -> int:
        if self.t not in [-TypeEnum.KJ]:
            raise ValueError(f"wrong type {self.t} for kJ")
        return struct.unpack("q", self.data)[0]

    def _databytes(self):
        if self.t == -TypeEnum.KS:
            return self.context.symbols_enc[struct.unpack("i", self.data)[0]] + b'\x00'
        return self.data

    def _paysz(self):
        return len(self.data)


class KIntArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KI):
        super().__init__(t)
        self.j = array.array("l", [0] * sz)
        self.attrib = 0

    def _paysz(self):
        return 1 + 4 + 4 * len(self.j)

    def _databytes(self):
        return struct.pack("<BI", self.attrib, len(self.j)) + struct.pack(f"<{len(self.j)}I", *self.j)


class KByteArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KB):
        super().__init__(t)
        self.g = array.array("b", [0] * sz)
        self.attrib = 0

    def _paysz(self):
        return 1 + 4 + 1 * len(self.g)

    def _databytes(self):
        return struct.pack("<BI", self.attrib, len(self.g)) + struct.pack(f"<{len(self.g)}B", *self.g)


class KListArray(KObj):
    def __init__(self):
        super().__init__(0)
        self.k = []
        self.attrib = 0

    def _paysz(self):
        # sum sizes nested ks
        return 1 + 4 + 1*len(self.k) + sum([ko._paysz() for ko in self.k])

    def _databytes(self):
        parts = [struct.pack("<BI", self.attrib, len(self.k))]
        for ko in self.k:
            parts.extend([struct.pack(f"<B", ko.t), ko._databytes()])
        return b"".join(parts)

def b9(k: KObj, msgtype=0, flags=0) -> bytes:
    # 8 byte header
    msglen = 8 + 1 + k._paysz()
    return struct.pack("<BBHIb", 1, msgtype, flags, msglen, k.t) + k._databytes()


# atom constructors
def ki(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KI).i(i)

def kj(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KJ).j(i)

def ks(s: str) -> KObj:
    return KObjAtom(-TypeEnum.KS).ss(s)


# vector constructors
def ktn(t: int, sz: int = 0) -> KObj:
    if t == TypeEnum.KI:
        return KIntArray(sz, t)
    if t == TypeEnum.KG:
        return KByteArray(sz, t)
    if t == 0:
        return KListArray()
    raise NotImplementedException(f"ktn for type {t}")

