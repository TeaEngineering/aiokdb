import array
import logging
import struct
import uuid
from collections.abc import MutableSequence, Sequence

from .enum import IntEnum

__all__ = [
    "b9",
    "d9",
    "ktn",
]

# mypy: disallow-untyped-defs

logger = logging.getLogger(__name__)


class WrongTypeForOperationError(TypeError):
    pass


class AttrEnum(IntEnum):
    NONE = 0
    SORTED = 1
    UNIQUE = 2
    PARTED = 3
    GROUPED = 4


class MessageType(IntEnum):
    ASYNC = 0
    SYNC = 1
    RESPONSE = 2


class Nulls:
    h = -32768
    i = -2147483648
    j = -9223372036854775808
    c = " "
    e = float("nan")
    f = float("nan")


class Infs:
    h = 32767
    i = 2147483647
    j = 9223372036854775807
    e = float("inf")
    f = float("inf")


class TypeEnum(IntEnum):
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
ATOM_LENGTH = {
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
    def __init__(self):
        self.symbols = {}
        self.symbols_enc = {}

    def ss(self, s):
        # we don't want any surprises trying to serialise non-ascii symbols later
        # so force non-ascii characters out now
        bs = bytes(s, "ascii") + b"\x00"
        idx = self.symbols.setdefault(s, len(self.symbols))
        # TODO this should be a list
        self.symbols_enc[idx] = (s, bs)
        return idx


DEFAULT_CONTEXT = KContext()


def tn(t):
    te = t
    if te != TypeEnum.KRR:
        te = abs(t)
    if te >= 20 and te < 40:
        return "Enum ({t})"
    return "{0} ({1})".format(TypeEnum(te).name, t)


class KObj:
    def __init__(
        self,
        t=0,
        context=DEFAULT_CONTEXT,
        sz=0,
        attr=0,
    ):
        self.t = t
        self.attrib = attr
        self.context = context

    def _paysz(self):
        return 1

    def _databytes(self):
        return struct.pack("<b", self.t)

    def _tn(self):
        return tn(self.t)

    def _te(self):
        return WrongTypeForOperationError("Not available for {0}".format(self._tn()))

    def __len__(self):
        raise self._te()

    # atom setters
    def ss(self, s):
        raise self._te()

    def b(self, b):
        raise self._te()

    def c(self, c):
        raise self._te()

    def g(self, g):
        raise self._te()

    def h(self, h):
        raise self._te()

    def i(self, i):
        raise self._te()

    def j(self, j):
        raise self._te()

    def uu(self, uu):
        raise self._te()

    # atom getters
    def aB(self):
        raise self._te()

    def aG(self):
        raise self._te()

    def aI(self):
        raise self._te()

    def aH(self):
        raise self._te()

    def aJ(self):
        raise self._te()

    def aS(self):
        raise self._te()

    def aU(self):
        raise self._te()

    def aE(self):
        raise self._te()

    def aF(self):
        raise self._te()

    def aC(self):
        raise self._te()

    # vector getters
    def kK(self):
        raise self._te()

    def kU(self):
        raise self._te()

    def kB(self):
        raise self._te()

    def kG(self):
        raise self._te()

    def kH(self):
        raise self._te()

    def kI(self):
        raise self._te()

    def kJ(self):
        raise self._te()

    def kE(self):
        raise self._te()

    def kF(self):
        raise self._te()

    def kC(self):  # type: ignore[type-arg]
        raise self._te()

    def kS(self):
        raise self._te()

    # dictionary/flip
    def kkey(self):
        raise self._te()

    def kvalue(self):
        raise self._te()

    def __getitem__(self, item):
        raise self._te()

    # TODO: clean this up by having kS() return a MutableSequence(str) that
    # writes through to the int array
    def appendS(self, *ss):
        raise self._te()

    # deserialise content from stream
    def frombytes(self, data, offset):
        raise self._te()

    def __eq__(self, other):
        # this could be optimised in subclasses with appropriate descent logic
        if isinstance(other, KObj):
            return b9(self) == b9(other)
        return NotImplemented


# constructors always take type t, optional context, and
# optionally a size, attr pair
class KObjAtom(KObj):
    def __init__(
        self,
        t=0,
        context=DEFAULT_CONTEXT,
        sz=0,
        attr=0,
    ):
        if t > 0 and t != TypeEnum.NIL:
            raise ValueError("Not atomic type {0}".format(t))
        super().__init__(t, context)
        self.data = b"\x00" * ATOM_LENGTH[-self.t]

    # atom setters
    def b(self, b):
        if self.t not in [-TypeEnum.KB]:
            raise ValueError("wrong type {0} for g()".format(self._tn()))
        self.data = struct.pack("B", {True: 1, False: 0}[b])
        return self

    def c(self, c):
        bs = c.encode("ascii")
        if self.t not in [-TypeEnum.KC]:
            raise ValueError("wrong type {0} for c()".format(self._tn()))
        if len(bs) != 1:
            raise ValueError(".c() takes single character")
        self.data = bs
        return self

    def g(self, g):
        if self.t not in [-TypeEnum.KG]:
            raise ValueError("wrong type {0} for g()".format(self._tn()))
        self.data = struct.pack("B", g)
        return self

    def h(self, h):
        if self.t not in [-TypeEnum.KH]:
            raise ValueError("wrong type {0} for h()".format(self._tn()))
        self.data = struct.pack("h", h)
        return self

    def i(self, i):
        if self.t not in [-TypeEnum.KI]:
            raise ValueError("wrong type {0} for i()".format(self._tn()))
        self.data = struct.pack("i", i)
        return self

    def j(self, j):
        if self.t not in [-TypeEnum.KJ, -TypeEnum.KP]:
            raise ValueError("wrong type {0} for j()".format(self._tn()))
        self.data = struct.pack("q", j)
        return self

    def uu(self, uu):
        if self.t not in [-TypeEnum.UU]:
            raise ValueError("wrong type {0} for uu()".format(self._tn()))
        self.data = uu.bytes
        return self

    # atom getters
    def aB(self):
        if self.t not in [-TypeEnum.KB]:
            raise ValueError("wrong type {0} for aB".format(self._tn()))
        return self.aG() == 1

    def aG(self):
        if self.t not in [-TypeEnum.KG, -TypeEnum.KB]:
            raise ValueError("wrong type {0} for aG".format(self._tn()))
        return struct.unpack("B", self.data)[0]

    def aH(self):
        if self.t not in [-TypeEnum.KH]:
            raise ValueError("wrong type {0} for aH".format(self._tn()))
        return struct.unpack("h", self.data)[0]

    def aI(self):
        if self.t not in [
            -TypeEnum.KI,
            -TypeEnum.KM,
            -TypeEnum.KD,
            -TypeEnum.KU,
            -TypeEnum.KV,
            -TypeEnum.KT,
        ]:
            raise ValueError("wrong type {0} for aI".format(self._tn()))
        return struct.unpack("i", self.data)[0]

    def aJ(self):
        if self.t not in [-TypeEnum.KJ, -TypeEnum.KP, -TypeEnum.KN]:
            raise ValueError("wrong type {0} for aJ".format(self._tn()))
        return struct.unpack("q", self.data)[0]

    def aU(self):
        if self.t not in [-TypeEnum.UU]:
            raise ValueError("wrong type {0} for aU".format(self._tn()))
        return uuid.UUID(bytes=self.data)

    def aE(self):
        if self.t not in [-TypeEnum.KE]:
            raise ValueError("wrong type {0} for aE".format(self._tn()))
        return struct.unpack("f", self.data)[0]

    def aF(self):
        if self.t not in [-TypeEnum.KF, -TypeEnum.KZ]:
            raise ValueError("wrong type {0} for aF".format(self._tn()))
        return struct.unpack("d", self.data)[0]

    def aC(self):
        if self.t not in [-TypeEnum.KC]:
            raise ValueError("wrong type {0} for aC".format(self._tn()))
        bs = struct.unpack("c", self.data)[0]
        return bs.decode("ascii")

    # serialisation
    def _databytes(self):
        return super()._databytes() + self.data

    def _paysz(self):
        assert len(self.data) == ATOM_LENGTH[-self.t]
        return super()._paysz() + ATOM_LENGTH[-self.t]

    def frombytes(self, data, offset):
        bs = ATOM_LENGTH[-self.t]
        self.data = data[offset : offset + bs]
        return self, offset + bs


class KSymAtom(KObj):
    def __init__(
        self,
        t=0,
        context=DEFAULT_CONTEXT,
        sz=0,
        attr=0,
    ):
        super().__init__(t, context)
        self.data = b""

    def aI(self):
        return struct.unpack("i", self.data)[0]

    def aS(self):
        return self.context.symbols_enc[self.aI()][0]

    def ss(self, s):
        if self.t not in [-TypeEnum.KS, TypeEnum.KRR]:
            raise ValueError("wrong type {0} for ss()".format(self._tn()))
        self.data = struct.pack("i", self.context.ss(s))
        return self

    # serialisation
    def _databytes(self):
        return super()._databytes() + self.context.symbols_enc[self.aI()][1]

    def _paysz(self):
        return super()._paysz() + len(self.context.symbols_enc[self.aI()][1])

    def frombytes(self, data, offset):
        bs = data[offset:].index(b"\x00") + 1
        self.ss(data[offset : offset + bs - 1].decode("ascii"))
        return self, offset + bs


class KErrAtom(KObj):
    def __init__(
        self,
        context=DEFAULT_CONTEXT,
    ):
        super().__init__(TypeEnum.KRR, context)
        self.data = b""

    def aS(self):
        return self.data[:-1].decode("ascii")

    def ss(self, s):
        self.data = bytes(s, "ascii") + b"\x00"
        return self

    # serialisation
    def _databytes(self):
        return super()._databytes() + self.data

    def _paysz(self):
        return super()._paysz() + len(self.data)

    def frombytes(self, data, offset):
        bs = data[offset:].index(b"\x00") + 1
        self.data = data[offset : offset + bs - 1]
        return self, offset + bs


class KRangedType(KObj):
    def frombytes(self, data, offset):
        attrib, sz = struct.unpack_from("<BI", data, offset=offset)
        self.attrib = attrib
        return self._ranged_frombytes(sz, data, offset + 5)

    def _ranged_frombytes(self, sz, data, offset):
        raise self._te()


class KByteArray(KRangedType):
    def __init__(self, t=TypeEnum.KG, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._g = array.array("B", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 1 * len(self._g)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self._g)) + struct.pack(
            "<{0}B".format(len(self._g)), *self._g
        )

    def kG(self):
        return self._g

    def __len__(self):
        return len(self._g)

    def _ranged_frombytes(self, sz, data, offset):
        self._g = array.array("B", data[offset : offset + sz])
        return self, offset + sz


class KShortArray(KRangedType):
    def __init__(self, t=TypeEnum.KH, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._h = array.array("h", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 2 * len(self._h)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self._h)) + struct.pack(
            "<{0}H".format(len(self._h)), *self._h
        )

    def kH(self):
        return self._h

    def __len__(self):
        return len(self._h)

    def _ranged_frombytes(self, sz, data, offset):
        self._h = array.array("h", data[offset : offset + 2 * sz])
        return self, offset + 2 * sz


class KIntArray(KRangedType):
    # python doesn't have a reliable 32-bit array type, "l" can either be 32 or 64 bits
    # which breaks direct cast-style deserialising. So when l is 64 bits we need to
    # iteratively unpack with unpack_from. Somewhat annoying given the other types map
    # cleanly.
    def __init__(self, t=TypeEnum.KI, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._i = array.array("l", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 4 * len(self._i)

    def _databytes(self):
        if self._i.itemsize == 4:  # array type 'l' seems to store as 8 bytes
            pi = self._i.tobytes()
        else:
            pi = struct.pack("<{0}I".format(len(self._i)), *self._i)
        return struct.pack("<bBI", self.t, self.attrib, len(self._i)) + pi

    def kI(self):
        return self._i

    def __len__(self):
        return len(self._i)

    def _ranged_frombytes(self, sz, data, offset):
        o2 = offset + 4 * sz
        if self._i.itemsize == 4:
            self._i = array.array("l", data[offset:o2])
        else:
            self._i = array.array("l", [0] * sz)
            for i in range(sz):
                self._i[i] = struct.unpack_from("<l", data, offset + 4 * i)[0]
        return self, o2


class KIntSymArray(KIntArray):
    # store symbol indexes in KIntArray
    # hook serialise to use null byte terminated representation
    def _paysz(self):
        return 2 + 4 + sum([len(self.context.symbols_enc[j][1]) for j in self._i])

    def _databytes(self):
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._i))]
        for j in self._i:
            parts.append(self.context.symbols_enc[j][1])
        return b"".join(parts)

    def kS(self):
        # Warning: accessor read-only
        s = []
        for j in self._i:
            s.append(self.context.symbols_enc[j][0])
        return s

    def appendS(self, *ss):
        for s in ss:
            j = self.context.ss(s)
            self._i.append(j)
        return self

    def __len__(self):
        return len(self._i)

    def _ranged_frombytes(self, sz, data, offset):
        self._i = array.array("l", [0] * sz)
        for i in range(sz):
            bs = data[offset:].index(b"\x00") + 1
            s = data[offset : offset + bs - 1].decode("ascii")
            d = self.context.ss(s)
            self._i[i] = d
            offset += bs
        return self, offset


class KLongArray(KRangedType):
    def __init__(self, t=TypeEnum.KJ, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._j = array.array("q", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 8 * len(self._j)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self._j)) + struct.pack(
            "<{0}q".format(len(self._j)), *self._j
        )

    def kJ(self):
        return self._j

    def __len__(self):
        return len(self._j)

    def _ranged_frombytes(self, sz, data, offset):
        self._j = array.array("q", data[offset : offset + 8 * sz])
        return self, offset + 8 * sz


class KFloatArray(KRangedType):
    def __init__(self, t=TypeEnum.KF, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._e = array.array("f", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 4 * len(self._e)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self._e)) + struct.pack(
            "<{0}f".format(len(self._e)), *self._e
        )

    def kE(self):
        return self._e

    def __len__(self):
        return len(self._e)

    def _ranged_frombytes(self, sz, data, offset):
        self._e = array.array("f", data[offset : offset + 4 * sz])
        return self, offset + 4 * sz


class KDoubleArray(KRangedType):
    def __init__(self, t=TypeEnum.KF, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._f = array.array("d", [0] * sz)

    def _paysz(self):
        return 2 + 4 + 8 * len(self._f)

    def _databytes(self):
        return struct.pack("<bBI", self.t, self.attrib, len(self._f)) + struct.pack(
            "<{0}d".format(len(self._f)), *self._f
        )

    def kF(self):
        return self._f

    def __len__(self):
        return len(self._f)

    def _ranged_frombytes(self, sz, data, offset):
        self._f = array.array("d", data[offset : offset + 8 * sz])
        return self, offset + 8 * sz


class KCharArray(KRangedType):
    def __init__(self, t=TypeEnum.KC, sz=0, attr=0):
        super().__init__(t, attr=attr)
        self._c = array.array("u", [" "] * sz)

    def _paysz(self):
        return 2 + 4 + 1 * len(self._c)

    def _databytes(self):
        bs = self._c.tounicode().encode("ascii")
        return struct.pack("<bBI", self.t, self.attrib, len(self._c)) + bs

    def kC(self):  # type: ignore[type-arg]
        return self._c

    def aS(self):
        return self._c.tounicode()

    def __len__(self):
        return len(self._c)

    def _ranged_frombytes(self, sz, data, offset):
        s = data[offset : offset + sz].decode("ascii")
        self._c = array.array("u", [])
        self._c.fromunicode(s)
        return self, offset + sz


class KObjArray(KRangedType):
    def __init__(self, t=TypeEnum.K):
        super().__init__(0)
        self._k = []

    def _paysz(self):
        # sum sizes nested ks
        return 2 + 4 + sum([ko._paysz() for ko in self._k])

    def _databytes(self):
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._k))]
        parts.extend(ko._databytes() for ko in self._k)
        return b"".join(parts)

    def kK(self):
        return self._k

    def __len__(self):
        return len(self._k)

    def _ranged_frombytes(self, sz, data, offset):
        for i in range(sz):
            obj, offset = _d9_unpackfrom(data, offset)
            self._k.append(obj)
        return self, offset


class KUUIDArray(KRangedType):
    def __init__(self, t=TypeEnum.UU, sz=0, attr=0):
        super().__init__(TypeEnum.UU)
        self._u = [uuid.UUID(int=0)] * sz

    def _paysz(self):
        # sum sizes nested ks
        return 2 + 4 + 16 * len(self._u)

    def _databytes(self):
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._u))]
        parts.extend(uu.bytes for uu in self._u)
        return b"".join(parts)

    def kU(self):
        return self._u

    def __len__(self):
        return len(self._u)

    def _ranged_frombytes(self, sz, data, offset):
        for i in range(sz):
            start = offset + i * 16
            self._u.append(uuid.UUID(bytes=data[start : start + 16]))
        return self, offset + 16 * sz


def b9(k, msgtype=0, flags=0):
    # 8 byte header
    msglen = 8 + k._paysz()
    return struct.pack("<BBHI", 1, msgtype, flags, msglen) + k._databytes()


def d9(data):
    # raises struct.error on underflow
    ver, msgtype, flags, msglen = struct.unpack_from("<BBHI", data, offset=0)
    if len(data) < msglen:
        raise ValueError(
            "buffer is too short, required {0} bytes, got {1}".format(msglen, len(data))
        )
    offset = 8
    if flags == 1:
        print(data.hex())
        data = decompress(data[8:])
        offset = 0
        msglen = len(data)
    elif flags != 0:
        print(data.hex())
        raise ValueError(
            f"unknown payload flags={flags} - not yet implemented, please open an Issue"
        )

    k, pos = _d9_unpackfrom(data, offset=offset)
    if pos != msglen:
        raise Exception("Final position at {0} expected {1}".format(pos, msglen))
    return k


def _d9_unpackfrom(data, offset):
    (t,) = struct.unpack_from("<b", data, offset=offset)
    offset += 1
    logger.debug(
        " at offset {0}/{1} unpacking type {2}".format(offset, len(data), tn(t))
    )
    if t == -TypeEnum.KS or t == TypeEnum.KRR:
        return KSymAtom(t).frombytes(data, offset)
    elif t < 0:
        return KObjAtom(t).frombytes(data, offset)
    elif t >= 0 and t < 20:
        # ranged vector types, need to verify t
        return VECTOR_CONSTUCTORS[t](t).frombytes(data, offset)
    elif t == TypeEnum.NIL:
        # seems to be a null byte after the type?
        return KObjAtom(t).frombytes(data, offset)
    elif t == TypeEnum.XD:
        kkeys, offset = _d9_unpackfrom(data, offset)
        kvalues, offset = _d9_unpackfrom(data, offset)
        return KDict(kkeys, kvalues), offset
    elif t == TypeEnum.XT:
        kkeys, offset = _d9_unpackfrom(data, offset + 1)
        return KFlip(kkeys), offset
    elif t == TypeEnum.SD:
        kkeys, offset = _d9_unpackfrom(data, offset)
        kvalues, offset = _d9_unpackfrom(data, offset)
        return KDict(kkeys, kvalues, t), offset
    elif t >= 20 and t < 30:
        return VECTOR_CONSTUCTORS[TypeEnum.KJ](t).frombytes(data, offset)
    raise ValueError("Unable to d9 unpack t={0}".format(t))


# atom constructors
def ka(t):
    return KObjAtom(t)


def kb(b):
    return KObjAtom(-TypeEnum.KB).b(b)


def kc(c):
    return KObjAtom(-TypeEnum.KC).c(c)


def kg(i):
    return KObjAtom(-TypeEnum.KG).g(i)


def kh(i):
    return KObjAtom(-TypeEnum.KH).h(i)


def ki(i):
    return KObjAtom(-TypeEnum.KI).i(i)


def kj(i):
    return KObjAtom(-TypeEnum.KJ).j(i)


def ks(s):
    return KSymAtom(-TypeEnum.KS).ss(s)


def kuu(uu):
    return KObjAtom(-TypeEnum.UU).uu(uu)


# vector constructors
VECTOR_CONSTUCTORS = {
    TypeEnum.K: KObjArray,
    TypeEnum.KB: KByteArray,
    TypeEnum.KG: KByteArray,
    TypeEnum.KH: KShortArray,
    TypeEnum.KI: KIntArray,
    TypeEnum.KJ: KLongArray,
    TypeEnum.KS: KIntSymArray,
    TypeEnum.UU: KUUIDArray,
    TypeEnum.KC: KCharArray,
    TypeEnum.KP: KLongArray,
    TypeEnum.KF: KDoubleArray,
    TypeEnum.KE: KFloatArray,
    TypeEnum.KM: KIntArray,
    TypeEnum.KD: KIntArray,
    TypeEnum.KZ: KDoubleArray,
    TypeEnum.KN: KLongArray,
    TypeEnum.KU: KIntArray,
    TypeEnum.KV: KIntArray,
    TypeEnum.KT: KIntArray,
}


def ktn(t, sz=0, attr=AttrEnum.NONE):
    if t == TypeEnum.K:
        if sz > 0:
            raise ValueError("ktn K can only be empty at initialisation")
        return KObjArray(t)
    try:
        return VECTOR_CONSTUCTORS[t](t, sz=sz, attr=attr)
    except KeyError:
        raise ValueError("ktn for type {0}".format(tn(t)))


def kk(*objs):
    k = ktn(TypeEnum.K)
    k.kK().extend(objs)
    return k


def cv(s):
    k = ktn(TypeEnum.KC)
    k.kC().fromunicode(s)
    return k


class KDict(KObj):
    def __init__(self, kkeys, kvalues, t=TypeEnum.XD):
        if len(kkeys) != len(kvalues):
            raise ValueError("dict keys and values must be same length")
        super().__init__(t)
        if t == TypeEnum.SD and kkeys.t != TypeEnum.XT and kkeys.attrib == 0:
            raise ValueError("Keys not sorted for SD {0}".format(kkeys._tn()))
        self._kkey = kkeys
        self._kvalue = kvalues

    def _paysz(self):
        return 1 + self._kkey._paysz() + self._kvalue._paysz()

    def _databytes(self):
        return (
            struct.pack("<B", self.t)
            + self._kkey._databytes()
            + self._kvalue._databytes()
        )

    def __len__(self):
        return len(self._kkey)

    def kkey(self):
        return self._kkey

    def kvalue(self):
        return self._kvalue

    def __getitem__(self, item):
        if self._kkey.t == TypeEnum.KS:
            try:
                idx = self.kkey().kS().index(item)
                return self.kvalue().kK()[idx]
            except ValueError:
                raise KeyError("Key not found {0}".format(item))
        elif self._kkey.t == TypeEnum.K:
            # check for any nested atomic symbol keys within the K object array
            for idx in range(len(self._kkey)):
                k = self._kkey.kK()[idx]
                if k.t == -TypeEnum.KS:
                    if k.aS() == item:
                        return self.kvalue().kK()[idx]
            raise KeyError("Key not found {0}".format(item))

        raise KeyError("Key lookup not possible on {0}".format(self._kkey._tn()))


class KFlip(KObj):
    def __init__(self, kd, sorted=False):
        if kd.t != TypeEnum.XD:
            raise ValueError("can only flip a dict, not {0}".format(kd._tn()))
        if len(kd.kkey()) == 0:
            raise ValueError("must have >0 columns")
        if kd.kvalue().t != TypeEnum.K:
            raise ValueError("must have K vector holding cols")
        if kd.kkey().t != TypeEnum.KS:
            raise ValueError("dict key must be S vector of column names")
        if len(kd.kvalue().kK()) == 0:
            raise ValueError("cannot have zero columns")

        attr = AttrEnum.NONE
        if sorted:
            attr = AttrEnum.SORTED
        super().__init__(TypeEnum.XT, attr=attr)
        self._kvalue = kd

    def _paysz(self):
        return 2 + self._kvalue._paysz()

    def _databytes(self):
        return struct.pack("<bB", self.t, self.attrib) + self._kvalue._databytes()

    def kvalue(self):
        return self._kvalue

    def __len__(self):
        # the length of a table is not the length of it's contained dictionary, as that is the number of columns.
        # we need to read-through to the first columns data.
        assert self.kvalue().t == TypeEnum.XD
        tdict_values = self.kvalue().kvalue()
        if len(tdict_values) == 0:
            raise ValueError("flip contains a dict with no values, so has no length")
        assert tdict_values.t == TypeEnum.K
        first_col = tdict_values.kK()[0]
        return len(first_col)

    def __getitem__(self, item):
        try:
            idx = self.kS().index(item)
            return self.kK()[idx]
        except ValueError:
            raise KeyError("Column not found {0}".format(item))

    def kS(self):
        # column names
        return self.kvalue().kkey().kS()

    def kK(self):
        # column values
        return self.kvalue().kvalue().kK()


def krr(msg):
    return KSymAtom(TypeEnum.KRR).ss(msg)


kNil = ka(TypeEnum.NIL)


class KException(Exception):
    pass


def xd(kkeys, kvalues, sorted=False):
    if sorted:
        return KDict(kkeys, kvalues, TypeEnum.SD)
    return KDict(kkeys, kvalues)


def xt(kd, sorted=False):
    return KFlip(kd, sorted=sorted)


def decompress(data: bytes) -> bytes:
    # decompression based on c.java
    # https://github.com/KxSystems/javakdb/blob/master/javakdb/src/main/java/com/kx/c.java#L667
    # which in turns seems quite similar to LZRW1 http://www.ross.net/compression/lzrw1.html
    # however with a much smaller window for the hash function (2 bytes) and only 256 slots

    uncomp_sz = struct.unpack("I", data[0:4])[0] - 8
    # print(f"decompressing: compressed {len(_data)} -> uncompressed {uncomp_sz}")

    # compressed stream repeats: control byte (8 single-bit instructions), followed by 8-16 data bytes.
    # Bits of the control byte are examined bit-by-bit from least significant:
    #   control bit clear: copy one literal byte to output.
    #   control bit set: read 2 bytes [index, sz]. Copy from hashed history table entry [index] for sz bytes.
    # After each operation, update the hashpos table provided we have at least 2 bytes output, and using no more
    # than two bytes from the history copy

    dst = bytearray(uncomp_sz)
    hashpos = [0] * 256
    d = 4  # position index into compressed data[]
    s = 0  # write position in dst[] (uncompressed output)
    p = 0  # hash  position in dst[] for hashpos updates
    f = 0  # current instruction byte, examined bit by bit
    i = 0  # bit position within f as a 1-hot value, ie. 1,2,4,8,16,32,64,128

    while s < uncomp_sz:
        if i == 0:
            # out of instruction bits! reload f, reset i, advance d
            # print(f"loading next 8 instructions from d={d}")
            f = data[d]
            d += 1
            i = 1

        # check bit i of f
        if f & i:  # bit set: copy history range
            # {ptr}{sz} copy n bytes (sz+2) from uncompressed history pointer r (ptr)
            r = hashpos[data[d]]
            n = data[d + 1]
            # print(f"instr pos {i} is copy history hashpos {data[d]} -> index {r} len {2+n}")
            # DO NOT USE SLICE ASSIGNMENT HERE, AS A HISTORY REFERENCE CAN COPY OWN OUTPUT
            # ie. start 8 bytes back and copy 64 bytes, gives 8x repeating 8 bytes
            for m in range(2 + n):
                dst[s + m] = dst[r + m]
            d += 2
            s += 2

        else:  # copy 1 byte from compressed stream to uncomp
            #  print(f"instr pos {i} literal copy byte d={d} value {data[d:d+1].hex()}")
            dst[s] = data[d]
            n = 0
            d += 1
            s += 1

        while p < s - 1:
            hashv = dst[p] ^ dst[p + 1]
            # print(f"hashpos slot {hashv} set to p={p} writepos {s}")
            hashpos[hashv] = p
            p += 1

        # only first two bytes of a copied range are used to update the hash
        # jump s and p over the remaining copied range (n)
        s += n
        p += n
        # next control bit
        i = (i << 1) & 255

    return dst
