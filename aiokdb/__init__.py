import array
import enum
import struct
import uuid
from collections.abc import MutableSequence, Sequence
from typing import Self, cast

__all__ = [
    "b9",
    "d9",
    "ktn",
]

# mypy: disallow-untyped-defs


class NotImplementedException(Exception):
    pass


class AttrEnum(enum.IntEnum):
    NONE = 0
    SORTED = 1
    UNIQUE = 2
    PARTED = 3
    GROUPED = 4


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


# Remember to special case TypeEnum.KS and TypeEnum.K
ATOM_LENGTH: dict[int, int] = {
    TypeEnum.KB: 1,
    TypeEnum.UU: 16,
    TypeEnum.KG: 1,
    TypeEnum.KH: 2,
    TypeEnum.KI: 4,
    TypeEnum.KJ: 8,
    TypeEnum.KE: 4,
    TypeEnum.KF: 8,
    TypeEnum.KC: 1,
    TypeEnum.KP: 8,
    TypeEnum.KM: 4,
    TypeEnum.KD: 4,
    TypeEnum.KN: 8,
    TypeEnum.KU: 4,
    TypeEnum.KV: 4,
    TypeEnum.KT: 4,
    TypeEnum.KZ: 8,
}


class KContext:
    def __init__(self) -> None:
        self.symbols: dict[str, int] = {}
        self.symbols_enc: dict[int, tuple[str, bytes]] = {}

    def ss(self, s: str) -> int:
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
    def __init__(
        self, t: int = 0, context: KContext = DEFAULT_CONTEXT, attr: int = 0
    ) -> None:
        self.t = t
        self.attrib = attr
        self.context: KContext = context

    def _paysz(self) -> int:
        return 1

    def _databytes(self) -> bytes:
        return struct.pack("<b", self.t)

    def _tn(self) -> str:
        return tn(self.t)

    def __len__(self) -> int:
        return 0

    # atom getters
    def aB(self) -> bool:
        raise NotImplementedException()

    def aG(self) -> int:
        raise NotImplementedException()

    def aI(self) -> int:
        raise NotImplementedException()

    def aH(self) -> int:
        raise NotImplementedException()

    def aJ(self) -> int:
        raise NotImplementedException()

    def aS(self) -> str:
        raise NotImplementedException()

    def aU(self) -> uuid.UUID:
        raise NotImplementedException()

    def aE(self) -> float:
        raise NotImplementedException()

    def aF(self) -> float:
        raise NotImplementedException()

    def aC(self) -> str:
        raise NotImplementedException()

    # vector getters
    def kK(self) -> MutableSequence["KObj"]:
        raise NotImplementedException()

    def kB(self) -> MutableSequence[bool]:
        raise NotImplementedException()

    def kG(self) -> MutableSequence[int]:
        raise NotImplementedException()

    def kH(self) -> MutableSequence[int]:
        raise NotImplementedException()

    def kI(self) -> MutableSequence[int]:
        raise NotImplementedException()

    def kJ(self) -> MutableSequence[int]:
        raise NotImplementedException()

    def kS(self) -> Sequence[str]:
        raise NotImplementedException()

    # TODO: clean this up by having kS() return a MutableSequence(str) that
    # writes through to the int array
    def appendS(self, *ss: str) -> Self:
        raise NotImplementedException()

    # deserialise content from stream
    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        raise NotImplementedException()


class KObjAtom(KObj):
    def __init__(self, t: int = 0, context: KContext = DEFAULT_CONTEXT) -> None:
        super().__init__(t, context)
        self.data: bytes = b""

    # atom setters
    def g(self, g: int) -> Self:
        if self.t not in [-TypeEnum.KG]:
            raise ValueError(f"wrong type {self._tn()} for g()")
        self.data = struct.pack("B", g)
        return self

    def h(self, h: int) -> Self:
        if self.t not in [-TypeEnum.KH]:
            raise ValueError(f"wrong type {self._tn()} for h()")
        self.data = struct.pack("h", h)
        return self

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
    def aB(self) -> bool:
        if self.t not in [-TypeEnum.KB]:
            raise ValueError(f"wrong type {self._tn()} for aB")
        return self.aG() == 1

    def aG(self) -> int:
        if self.t not in [-TypeEnum.KG, -TypeEnum.KB]:
            raise ValueError(f"wrong type {self._tn()} for aG")
        return cast(int, struct.unpack("B", self.data)[0])

    def aH(self) -> int:
        if self.t not in [-TypeEnum.KH]:
            raise ValueError(f"wrong type {self._tn()} for aH")
        return cast(int, struct.unpack("h", self.data)[0])

    def aI(self) -> int:
        if self.t not in [-TypeEnum.KI, -TypeEnum.KS]:
            raise ValueError(f"wrong type {self._tn()} for aI")
        return cast(int, struct.unpack("i", self.data)[0])

    def aJ(self) -> int:
        if self.t not in [-TypeEnum.KJ]:
            raise ValueError(f"wrong type {self._tn()} for aJ")
        return cast(int, struct.unpack("q", self.data)[0])

    def aS(self) -> str:
        return self.context.symbols_enc[self.aI()][0]

    def aU(self) -> uuid.UUID:
        if self.t not in [-TypeEnum.UU]:
            raise ValueError(f"wrong type {self._tn()} for aU")
        return uuid.UUID(bytes=self.data)

    def aE(self) -> float:
        if self.t not in [-TypeEnum.KE]:
            raise ValueError(f"wrong type {self._tn()} for aE")
        return cast(float, struct.unpack("f", self.data)[0])

    def aF(self) -> float:
        if self.t not in [-TypeEnum.KF]:
            raise ValueError(f"wrong type {self._tn()} for aF")
        return cast(float, struct.unpack("d", self.data)[0])

    def aC(self) -> str:
        if self.t not in [-TypeEnum.KC]:
            raise ValueError(f"wrong type {self._tn()} for aC")
        bs = struct.unpack("c", self.data)[0]
        return cast(str, bs.decode("ascii"))

    # serialisation
    def _databytes(self) -> bytes:
        bs = self.data
        if self.t == -TypeEnum.KS:
            bs = self.context.symbols_enc[self.aI()][1]
        return super()._databytes() + bs

    def _paysz(self) -> int:
        if self.t == -TypeEnum.KS:
            sz = len(self.context.symbols_enc[self.aI()][1])
        else:
            sz = ATOM_LENGTH[-self.t]
            assert len(self.data) == sz
        return super()._paysz() + sz

    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        offset += 1
        if self.t == -TypeEnum.KS:
            bs = data[offset:].index(b"\x00") + 1
            self.ss(data[offset : offset + bs - 1].decode("ascii"))
        else:
            bs = ATOM_LENGTH[-self.t]
            self.data = data[offset : offset + bs]
        return self, offset + bs


class KByteArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KB, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self.g = array.array("B", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 1 * len(self.g)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self.g)) + struct.pack(
            f"<{len(self.g)}B", *self.g
        )

    def kG(self) -> MutableSequence[int]:
        return self.g

    def __len__(self) -> int:
        return len(self.g)

    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        # t, attrib, sz = struct.unpack_from("<bBI", data, offset=offset)
        offset += 6
        self.g = array.array("B", data[offset : offset + len(self)])
        return self, offset + len(self)


class KShortArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KH, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self.h = array.array("h", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 2 * len(self.h)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self.h)) + struct.pack(
            f"<{len(self.h)}H", *self.h
        )

    def kH(self) -> MutableSequence[int]:
        return self.h

    def __len__(self) -> int:
        return len(self.h)


class KIntArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KI, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self.i = array.array("l", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 4 * len(self.i)

    def _databytes(self) -> bytes:
        if self.i.itemsize == 4:  # array type 'l' seems to store as 8 bytes
            pi = self.i.tobytes()
        else:
            pi = struct.pack(f"<{len(self.i)}I", *self.i)
        return struct.pack("<bBI", self.t, self.attrib, len(self.i)) + pi

    def kI(self) -> MutableSequence[int]:
        return self.i

    def __len__(self) -> int:
        return len(self.i)


class KIntSymArray(KIntArray):
    # store symbol indexes in KIntArray
    # hook serialise to use null byte terminated representation
    def _paysz(self) -> int:
        return 2 + 4 + sum([len(self.context.symbols_enc[j][1]) for j in self.i])

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self.i))]
        for j in self.i:
            parts.append(self.context.symbols_enc[j][1])
        return b"".join(parts)

    def kS(self) -> Sequence[str]:
        # Warning: accessor read-only
        s = []
        for j in self.i:
            s.append(self.context.symbols_enc[j][0])
        return s

    def appendS(self, *ss: str) -> Self:
        for s in ss:
            j = self.context.ss(s)
            self.i.append(j)
        return self

    def __len__(self) -> int:
        return len(self.i)


class KLongArray(KObj):
    def __init__(self, sz: int, t: int = TypeEnum.KJ, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self.j = array.array("q", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 8 * len(self.j)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self.j)) + struct.pack(
            f"<{len(self.j)}q", *self.j
        )

    def kJ(self) -> MutableSequence[int]:
        return self.j

    def __len__(self) -> int:
        return len(self.j)


class KObjArray(KObj):
    def __init__(self) -> None:
        super().__init__(0)
        self.k: list[KObj] = []

    def _paysz(self) -> int:
        # sum sizes nested ks
        return 2 + 4 + sum([ko._paysz() for ko in self.k])

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self.k))]
        parts.extend(ko._databytes() for ko in self.k)
        return b"".join(parts)

    def kK(self) -> MutableSequence[KObj]:
        return self.k

    def __len__(self) -> int:
        return len(self.k)


def b9(k: KObj, msgtype: int = 0, flags: int = 0) -> bytes:
    # 8 byte header
    msglen = 8 + k._paysz()
    return struct.pack("<BBHI", 1, msgtype, flags, msglen) + k._databytes()


def d9(data: bytes) -> KObj:
    # raises struct.error on underflow
    ver, msgtype, flags, msglen = struct.unpack_from("<BBHI", data, offset=0)
    if len(data) < msglen:
        raise ValueError(
            f"buffer is too short, required {msglen} bytes, got {len(data)}"
        )
    k, pos = _d9_unpackfrom(data, offset=8)
    if pos != msglen:
        raise Exception(f"Final position at {pos} expected {msglen}")
    return k


def _d9_unpackfrom(data: bytes, offset: int) -> tuple[KObj, int]:
    (t,) = struct.unpack_from("<b", data, offset=offset)
    if t < 0:
        ka = KObjAtom(t)
        return ka.frombytes(data, offset)
    elif t >= 0 and t <= 15:
        # ranged vector types
        t, attrib, sz = struct.unpack_from("<bBI", data, offset=offset)
        k = ktn(t, sz, attrib)
        return k.frombytes(data, offset)

    raise NotImplementedException(f"Unable to d9 unpack t={t}")


# atom constructors
def kg(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KG).g(i)


def kh(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KH).h(i)


def ki(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KI).i(i)


def kj(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KJ).j(i)


def ks(s: str) -> KObj:
    return KObjAtom(-TypeEnum.KS).ss(s)


# vector constructors
def ktn(t: TypeEnum, sz: int = 0, attr: AttrEnum = AttrEnum.NONE) -> KObj:
    if t == TypeEnum.KB:
        return KByteArray(sz, t, attr)
    if t == TypeEnum.KG:
        return KByteArray(sz, t, attr)
    if t == TypeEnum.KH:
        return KShortArray(sz, t, attr)
    if t == TypeEnum.KI:
        return KIntArray(sz, t, attr)
    if t == TypeEnum.KJ:
        return KLongArray(sz, t, attr)
    if t == TypeEnum.KS:
        return KIntSymArray(sz, t, attr)
    if t == TypeEnum.K:
        return KObjArray()

    raise NotImplementedException(f"ktn for type {tn(t)}")


class KDict(KObj):
    def __init__(self, kkeys: KObj, kvalues: KObj, t: TypeEnum = TypeEnum.XD):
        super().__init__(t)
        if t == TypeEnum.SD and kkeys.t != TypeEnum.XT and kkeys.attrib == 0:
            raise ValueError(f"Keys not sorted for SD {kkeys._tn()}")
        self.kkeys = kkeys
        self.kvalues = kvalues

    def _paysz(self) -> int:
        return 1 + self.kkeys._paysz() + self.kvalues._paysz()

    def _databytes(self) -> bytes:
        return (
            struct.pack("<B", self.t)
            + self.kkeys._databytes()
            + self.kvalues._databytes()
        )

    def __len__(self) -> int:
        return len(self.kkeys)


class KFlip(KObj):
    def __init__(self, kd: KDict, sorted: bool = False):
        attr = AttrEnum.NONE
        if sorted:
            attr = AttrEnum.SORTED
            assert kd.t == TypeEnum.XD
        super().__init__(TypeEnum.XT, attr=attr)
        self.kd = kd

    def _paysz(self) -> int:
        return 2 + self.kd._paysz()

    def _databytes(self) -> bytes:
        return struct.pack("<bB", self.t, self.attrib) + self.kd._databytes()


def xd(kkeys: KObj, kvalues: KObj, sorted: bool = False) -> KDict:
    if sorted:
        return KDict(kkeys, kvalues, TypeEnum.SD)
    return KDict(kkeys, kvalues)


def xt(kd: KDict, sorted: bool = False) -> KFlip:
    return KFlip(kd, sorted=sorted)
