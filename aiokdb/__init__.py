import array
import enum
import struct
import uuid
from collections.abc import MutableSequence, Sequence
from typing import Self, Type, cast

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


class MessageType(enum.IntEnum):
    ASYNC = 0
    SYNC = 1
    RESPONSE = 2


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
    KZ = 15  # 8    datetime  double kF (DO NOT USE)
    KN = 16  # 8    timespan  long   kJ (nanoseconds)
    KU = 17  # 4    minute    int    kI
    KV = 18  # 4    second    int    kI
    KT = 19  # 4    time      int    kI (millisecond)
    XT = 98  # table
    XD = 99  # dict
    FN = 100  # function
    NIL = 101  # nil item
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
    -TypeEnum.NIL: 1,  # TODO: unhack this
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
    te = t
    if te != TypeEnum.KRR:
        te = abs(t)
    return f"{TypeEnum(te).name} ({t})"


class KObj:
    def __init__(
        self,
        t: int = 0,
        context: KContext = DEFAULT_CONTEXT,
        sz: int = 0,
        attr: int = 0,
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

    def kU(self) -> MutableSequence[uuid.UUID]:
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

    def kC(self) -> array.array:  # type: ignore[type-arg]
        raise NotImplementedException()

    def kS(self) -> Sequence[str]:
        raise NotImplementedException()

    # dictionary/flip
    def kkey(self) -> "KObj":
        raise NotImplementedException()

    def kvalue(self) -> "KObj":
        raise NotImplementedException()

    # TODO: clean this up by having kS() return a MutableSequence(str) that
    # writes through to the int array
    def appendS(self, *ss: str) -> Self:
        raise NotImplementedException()

    # deserialise content from stream
    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        raise NotImplementedException()


# constructors always take type t, optional context, and
# optionally a size, attr pair
class KObjAtom(KObj):
    def __init__(
        self,
        t: int = 0,
        context: KContext = DEFAULT_CONTEXT,
        sz: int = 0,
        attr: int = 0,
    ) -> None:
        super().__init__(t, context)
        self.data: bytes = b"\x00" * ATOM_LENGTH[-self.t]

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
        if self.t not in [-TypeEnum.KI]:
            raise ValueError(f"wrong type {self._tn()} for aI")
        return cast(int, struct.unpack("i", self.data)[0])

    def aJ(self) -> int:
        if self.t not in [-TypeEnum.KJ]:
            raise ValueError(f"wrong type {self._tn()} for aJ")
        return cast(int, struct.unpack("q", self.data)[0])

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
        return super()._databytes() + self.data

    def _paysz(self) -> int:
        assert len(self.data) == ATOM_LENGTH[-self.t]
        return super()._paysz() + ATOM_LENGTH[-self.t]

    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        bs = ATOM_LENGTH[-self.t]
        self.data = data[offset : offset + bs]
        return self, offset + bs


class KSymAtom(KObj):
    def __init__(
        self,
        t: int = 0,
        context: KContext = DEFAULT_CONTEXT,
        sz: int = 0,
        attr: int = 0,
    ) -> None:
        super().__init__(t, context)
        self.data: bytes = b""

    def aI(self) -> int:
        return cast(int, struct.unpack("i", self.data)[0])

    def aS(self) -> str:
        return self.context.symbols_enc[self.aI()][0]

    def ss(self, s: str) -> Self:
        if self.t not in [-TypeEnum.KS, TypeEnum.KRR]:
            raise ValueError(f"wrong type {self._tn()} for ss()")
        self.data = struct.pack("i", self.context.ss(s))
        return self

    # serialisation
    def _databytes(self) -> bytes:
        return super()._databytes() + self.context.symbols_enc[self.aI()][1]

    def _paysz(self) -> int:
        return super()._paysz() + len(self.context.symbols_enc[self.aI()][1])

    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        bs = data[offset:].index(b"\x00") + 1
        self.ss(data[offset : offset + bs - 1].decode("ascii"))
        return self, offset + bs


class KErrAtom(KObj):
    def __init__(
        self,
        context: KContext = DEFAULT_CONTEXT,
    ) -> None:
        super().__init__(TypeEnum.KRR, context)
        self.data: bytes = b""

    def aS(self) -> str:
        return self.data[:-1].decode("ascii")

    def ss(self, s: str) -> Self:
        self.data = bytes(s, "ascii") + b"\x00"
        return self

    # serialisation
    def _databytes(self) -> bytes:
        return super()._databytes() + self.data

    def _paysz(self) -> int:
        return super()._paysz() + len(self.data)

    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        bs = data[offset:].index(b"\x00") + 1
        self.data = data[offset : offset + bs - 1]
        return self, offset + bs


class KRangedType(KObj):
    def frombytes(self, data: bytes, offset: int) -> tuple[Self, int]:
        attrib, sz = struct.unpack_from("<BI", data, offset=offset)
        self.attrib = attrib
        return self._ranged_frombytes(sz, data, offset + 5)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        raise NotImplementedException()


class KByteArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KG, sz: int = 0, attr: int = 0) -> None:
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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        self.g = array.array("B", data[offset : offset + sz])
        return self, offset + sz


class KShortArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KH, sz: int = 0, attr: int = 0) -> None:
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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        self.h = array.array("h", data[offset : offset + 2 * sz])
        return self, offset + 2 * sz


class KIntArray(KRangedType):
    # python doesn't have a reliable 32-bit array type, "l" can either be 32 or 64 bits
    # which breaks direct cast-style deserialising. So when l is 64 bits we need to
    # iteratively unpack with unpack_from. Somewhat annoying given the other types map
    # cleanly.
    def __init__(self, t: int = TypeEnum.KI, sz: int = 0, attr: int = 0) -> None:
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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        o2 = offset + 4 * sz
        if self.i.itemsize == 4:
            self.i = array.array("l", data[offset:o2])
        else:
            self.i = array.array("l", [0] * sz)
            for i in range(sz):
                self.i[i] = struct.unpack_from("<l", data, offset + 4 * i)[0]
        return self, o2


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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        self.i = array.array("l", [0] * sz)
        for i in range(sz):
            bs = data[offset:].index(b"\x00") + 1
            s = data[offset : offset + bs - 1].decode("ascii")
            d = self.context.ss(s)
            self.i[i] = d
            offset += bs
        return self, offset


class KLongArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KJ, sz: int = 0, attr: int = 0) -> None:
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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        self.j = array.array("q", data[offset : offset + 8 * sz])
        return self, offset + 8 * sz


class KCharArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KJ, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self.j: array.array[str] = array.array("u", [" "] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 1 * len(self.j)

    def _databytes(self) -> bytes:
        bs = self.j.tounicode().encode("ascii")
        return struct.pack("<bBI", self.t, self.attrib, len(self.j)) + bs

    def kC(self) -> array.array:  # type: ignore[type-arg]
        return self.j

    def aS(self) -> str:
        return self.j.tounicode()

    def __len__(self) -> int:
        return len(self.j)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        s = data[offset : offset + sz].decode("ascii")
        self.j = array.array("u", [])
        self.j.fromunicode(s)
        return self, offset + sz


class KObjArray(KRangedType):
    def __init__(self, t: int = TypeEnum.K) -> None:
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

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        for i in range(sz):
            obj, offset = _d9_unpackfrom(data, offset)
            self.k.append(obj)
        return self, offset


class KUUIDArray(KRangedType):
    def __init__(self, t: int = TypeEnum.UU) -> None:
        super().__init__(TypeEnum.UU)
        self.k: list[uuid.UUID] = []

    def _paysz(self) -> int:
        # sum sizes nested ks
        return 2 + 4 + 8 * len(self.k)

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self.k))]
        parts.extend(uu.bytes for uu in self.k)
        return b"".join(parts)

    def kU(self) -> MutableSequence[uuid.UUID]:
        return self.k

    def __len__(self) -> int:
        return len(self.k)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> tuple[Self, int]:
        for i in range(sz):
            self.k.append(uuid.UUID(bytes=data[offset + 16 * i : offset + 16 + 16 * i]))
        return self, offset + 16 * sz


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
    offset += 1
    # print(f" at offset {offset} unpacking type {tn(t)}")
    if t == -TypeEnum.KS or t == TypeEnum.KRR:
        return KSymAtom(t).frombytes(data, offset)
    elif t < 0:
        return KObjAtom(t).frombytes(data, offset)
    elif t >= 0 and t <= 15:
        # ranged vector types, need to verify t
        return VECTOR_CONSTUCTORS[t](t).frombytes(data, offset)
    elif t == TypeEnum.NIL:
        # seems to be a null byte after the type?
        return KObjAtom(t).frombytes(data, offset)
    elif t == TypeEnum.XD:
        kkeys, offset = _d9_unpackfrom(data, offset)
        kvalues, offset = _d9_unpackfrom(data, offset)
        return KDict(kkeys, kvalues), offset

    raise ValueError(f"Unable to d9 unpack t={t}")


# atom constructors
def ka(t: TypeEnum) -> KObj:
    return KObjAtom(t)


def kg(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KG).g(i)


def kh(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KH).h(i)


def ki(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KI).i(i)


def kj(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KJ).j(i)


def ks(s: str) -> KObj:
    return KSymAtom(-TypeEnum.KS).ss(s)


# vector constructors
VECTOR_CONSTUCTORS: dict[TypeEnum, Type[KObj]] = {
    TypeEnum.K: KObjArray,
    TypeEnum.KB: KByteArray,
    TypeEnum.KG: KByteArray,
    TypeEnum.KH: KShortArray,
    TypeEnum.KI: KIntArray,
    TypeEnum.KJ: KLongArray,
    TypeEnum.KS: KIntSymArray,
    TypeEnum.UU: KUUIDArray,
    TypeEnum.KC: KCharArray,
}


def ktn(t: TypeEnum, sz: int = 0, attr: AttrEnum = AttrEnum.NONE) -> KObj:
    if t == TypeEnum.K:
        if sz > 0:
            raise ValueError("ktn K can only be empty at initialisation")
        return KObjArray(t)

    try:
        return VECTOR_CONSTUCTORS[t](t, sz=sz, attr=attr)
    except KeyError:
        raise ValueError(f"ktn for type {tn(t)}")


def cv(s: str) -> KObj:
    k = ktn(TypeEnum.KC)
    k.kC().fromunicode(s)
    return k


class KDict(KObj):
    def __init__(self, kkeys: KObj, kvalues: KObj, t: TypeEnum = TypeEnum.XD):
        super().__init__(t)
        if t == TypeEnum.SD and kkeys.t != TypeEnum.XT and kkeys.attrib == 0:
            raise ValueError(f"Keys not sorted for SD {kkeys._tn()}")
        self._kkey = kkeys
        self._kvalue = kvalues

    def _paysz(self) -> int:
        return 1 + self._kkey._paysz() + self._kvalue._paysz()

    def _databytes(self) -> bytes:
        return (
            struct.pack("<B", self.t)
            + self._kkey._databytes()
            + self._kvalue._databytes()
        )

    def __len__(self) -> int:
        return len(self._kkey)

    def kkey(self) -> KObj:
        return self._kkey

    def kvalue(self) -> KObj:
        return self._kvalue


class KFlip(KObj):
    def __init__(self, kd: KDict, sorted: bool = False):
        attr = AttrEnum.NONE
        if sorted:
            attr = AttrEnum.SORTED
            assert kd.t == TypeEnum.XD
        super().__init__(TypeEnum.XT, attr=attr)
        self._kvalue = kd

    def _paysz(self) -> int:
        return 2 + self._kvalue._paysz()

    def _databytes(self) -> bytes:
        return struct.pack("<bB", self.t, self.attrib) + self._kvalue._databytes()

    def kvalue(self) -> KObj:
        return self._kvalue


def krr(msg: str) -> KSymAtom:
    return KSymAtom(TypeEnum.KRR).ss(msg)


class KException(Exception):
    pass


def xd(kkeys: KObj, kvalues: KObj, sorted: bool = False) -> KDict:
    if sorted:
        return KDict(kkeys, kvalues, TypeEnum.SD)
    return KDict(kkeys, kvalues)


def xt(kd: KDict, sorted: bool = False) -> KFlip:
    return KFlip(kd, sorted=sorted)
