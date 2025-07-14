import array
import enum
import logging
import struct
import uuid
from collections.abc import MutableSequence
from typing import Any, Dict, List, Tuple, Type, Union, cast

from aiokdb.adapter import BoolByteAdaptor, SymIntAdaptor
from aiokdb.compress import decompress
from aiokdb.context import KContext

__all__ = [
    "b9",
    "d9",
    "ktn",
    "kk",
    "cv",
    "MessageType",
    "Nulls",
    "Infs",
    "TypeEnum",
    "KObj",
    "KFnAtom",
    "xd",
    "xt",
    "ka",
    "kb",
    "kc",
    "kg",
    "kh",
    "ki",
    "kj",
    "ks",
    "kuu",
    "KContext",
]

# mypy: disallow-untyped-defs

logger = logging.getLogger(__name__)


class WrongTypeForOperationError(TypeError):
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


class Nulls:
    h: int = -32768
    i: int = -2147483648
    j: int = -9223372036854775808
    c: str = " "
    e: float = float("nan")
    f: float = float("nan")


class Infs:
    h: int = 32767
    i: int = 2147483647
    j: int = 9223372036854775807
    e: float = float("inf")
    f: float = float("inf")


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
    OP = 102  # builtin operator
    SD = 127  # sorted dict
    KRR = -128  # error


# Remember to special case TypeEnum.KS and TypeEnum.K
ATOM_LENGTH: Dict[int, int] = {
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


DEFAULT_CONTEXT = KContext()


def tn(t: int) -> str:
    te = t
    if te != TypeEnum.KRR:
        te = abs(t)
    if te >= 20 and te < 40:
        return "Enum ({t})"
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

    def _te(self) -> Exception:
        return WrongTypeForOperationError(f"Not available for {self._tn()}")

    def _te_repr(self) -> str:
        return f"TypeEnum.{TypeEnum(self.t).name}"

    def __len__(self) -> int:
        raise self._te()

    # atom setters
    def ss(self, s: str) -> "KObj":
        raise self._te()

    def b(self, b: bool) -> "KObj":
        raise self._te()

    def c(self, c: str) -> "KObj":
        raise self._te()

    def f(self, f: float) -> "KObj":
        raise self._te()

    def g(self, g: int) -> "KObj":
        raise self._te()

    def h(self, h: int) -> "KObj":
        raise self._te()

    def i(self, i: int) -> "KObj":
        raise self._te()

    def j(self, j: int) -> "KObj":
        raise self._te()

    def uu(self, uu: uuid.UUID) -> "KObj":
        raise self._te()

    # atom getters
    def aB(self) -> bool:
        raise self._te()

    def aG(self) -> int:
        raise self._te()

    def aI(self) -> int:
        raise self._te()

    def aH(self) -> int:
        raise self._te()

    def aJ(self) -> int:
        raise self._te()

    def aS(self) -> str:
        raise self._te()

    def aU(self) -> uuid.UUID:
        raise self._te()

    def aE(self) -> float:
        raise self._te()

    def aF(self) -> float:
        raise self._te()

    def aC(self) -> str:
        raise self._te()

    # vector getters
    def kK(self) -> "MutableSequence[KObj]":
        raise self._te()

    def kU(self) -> "MutableSequence[uuid.UUID]":
        raise self._te()

    def kB(self) -> "MutableSequence[bool]":
        raise self._te()

    def kG(self) -> "MutableSequence[int]":
        raise self._te()

    def kH(self) -> "MutableSequence[int]":
        raise self._te()

    def kI(self) -> "MutableSequence[int]":
        raise self._te()

    def kJ(self) -> "MutableSequence[int]":
        raise self._te()

    def kE(self) -> "MutableSequence[float]":
        raise self._te()

    def kF(self) -> "MutableSequence[float]":
        raise self._te()

    def kC(self) -> array.array:  # type: ignore[type-arg]
        raise self._te()

    def kS(self) -> "MutableSequence[str]":
        raise self._te()

    # dictionary/flip
    def kkey(self) -> "KObj":
        raise self._te()

    def kvalue(self) -> "KObj":
        raise self._te()

    def __getitem__(self, item: Union[int, str]) -> "KObj":
        raise self._te()

    # TODO: clean this up by having kS() return a MutableSequence(str) that
    # writes through to the int array
    def appendS(self, *ss: str) -> "KObj":
        raise self._te()

    # deserialise content from stream
    def frombytes(self, data: bytes, offset: int) -> Tuple["KObj", int]:
        raise self._te()

    def __eq__(self, other: Any) -> bool:
        # this could be optimised in subclasses with appropriate descent logic
        if isinstance(other, KObj):
            return b9(self) == b9(other)
        return NotImplemented

    def __repr__(self) -> str:
        raise self._te()

    def __str__(self) -> str:
        klass = self.__class__
        return f"<{klass.__module__}.{klass.__qualname__} object at {hex(id(self))}>"


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
        if t > 0 and t != TypeEnum.NIL:
            raise ValueError(f"Not atomic type {t}")
        super().__init__(t, context)
        self.data: bytes = b"\x00" * ATOM_LENGTH[-self.t]

    # atom setters
    def b(self, b: bool) -> KObj:
        if self.t not in [-TypeEnum.KB]:
            raise ValueError(f"wrong type {self._tn()} for g()")
        self.data = struct.pack("B", {True: 1, False: 0}[b])
        return self

    def c(self, c: str) -> KObj:
        if self.t not in [-TypeEnum.KC]:
            raise ValueError(f"wrong type {self._tn()} for c()")
        bs = c.encode("utf-8")
        if len(bs) != 1:
            raise ValueError(".c() takes single ASCII character")
        self.data = bs
        return self

    def f(self, f: float) -> KObj:
        if self.t == -TypeEnum.KE:
            self.data = struct.pack("<f", f)
        elif self.t in [-TypeEnum.KF, -TypeEnum.KZ]:
            self.data = struct.pack("<d", f)
        else:
            raise ValueError(f"wrong type {self._tn()} for f()")
        return self

    def g(self, g: int) -> KObj:
        if self.t not in [-TypeEnum.KG]:
            raise ValueError(f"wrong type {self._tn()} for g()")
        self.data = struct.pack("B", g)
        return self

    def h(self, h: int) -> KObj:
        if self.t not in [-TypeEnum.KH]:
            raise ValueError(f"wrong type {self._tn()} for h()")
        self.data = struct.pack("h", h)
        return self

    def i(self, i: int) -> KObj:
        if self.t not in [-TypeEnum.KI]:
            raise ValueError(f"wrong type {self._tn()} for i()")
        self.data = struct.pack("i", i)
        return self

    def j(self, j: int) -> KObj:
        if self.t not in [-TypeEnum.KJ, -TypeEnum.KP]:
            raise ValueError(f"wrong type {self._tn()} for j()")
        self.data = struct.pack("q", j)
        return self

    def uu(self, uu: uuid.UUID) -> KObj:
        if self.t not in [-TypeEnum.UU]:
            raise ValueError(f"wrong type {self._tn()} for uu()")
        self.data = uu.bytes
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
        if self.t not in [
            -TypeEnum.KI,
            -TypeEnum.KM,
            -TypeEnum.KD,
            -TypeEnum.KU,
            -TypeEnum.KV,
            -TypeEnum.KT,
        ]:
            raise ValueError(f"wrong type {self._tn()} for aI")
        return cast(int, struct.unpack("i", self.data)[0])

    def aJ(self) -> int:
        if self.t not in [-TypeEnum.KJ, -TypeEnum.KP, -TypeEnum.KN]:
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
        if self.t not in [-TypeEnum.KF, -TypeEnum.KZ]:
            raise ValueError(f"wrong type {self._tn()} for aF")
        return cast(float, struct.unpack("d", self.data)[0])

    def aC(self) -> str:
        if self.t not in [-TypeEnum.KC]:
            raise ValueError(f"wrong type {self._tn()} for aC")
        return self.data.decode("utf-8")

    # serialisation
    def _databytes(self) -> bytes:
        return super()._databytes() + self.data

    def _paysz(self) -> int:
        assert len(self.data) == ATOM_LENGTH[-self.t]
        return super()._paysz() + ATOM_LENGTH[-self.t]

    def frombytes(self, data: bytes, offset: int) -> Tuple[KObj, int]:
        bs = ATOM_LENGTH[-self.t]
        self.data = data[offset : offset + bs]
        return self, offset + bs

    def __repr__(self) -> str:
        if self.t == -TypeEnum.KI:
            return f"ki({self.aI()})"
        elif self.t == -TypeEnum.KJ:
            return f"kj({self.aJ()})"
        elif self.t == -TypeEnum.KB:
            return f"kb({self.aB()})"
        elif self.t == -TypeEnum.KG:
            return f"kg({self.aG()})"
        elif self.t == -TypeEnum.KE:
            return f"ke({repr(self.aE())})"
        elif self.t == -TypeEnum.KF:
            return f"kf({repr(self.aF())})"
        elif self.t == -TypeEnum.KH:
            return f"kh({self.aH()})"
        elif self.t == -TypeEnum.KC:
            return f"kc({repr(self.aC())})"
        elif self.t == TypeEnum.NIL:
            return "ka(TypeEnum.NIL)"
        elif self.t == -TypeEnum.UU:
            return f"kuu({repr(self.aU())})"
        else:
            raise Exception(f"__repr__ NYI for t={self.t}")


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
        return self.context.lookup_str(self.aI())

    def ss(self, s: str) -> KObj:
        if self.t not in [-TypeEnum.KS, TypeEnum.KRR]:
            raise ValueError(f"wrong type {self._tn()} for ss()")
        self.data = struct.pack("i", self.context.ss(s))
        return self

    # serialisation
    def _databytes(self) -> bytes:
        return super()._databytes() + self.context.lookup_bytes(self.aI())

    def _paysz(self) -> int:
        return super()._paysz() + len(self.context.lookup_bytes(self.aI()))

    def frombytes(self, data: bytes, offset: int) -> Tuple[KObj, int]:
        bs = data[offset:].index(b"\x00") + 1
        self.ss(data[offset : offset + bs - 1].decode("utf-8"))
        return self, offset + bs

    def __repr__(self) -> str:
        if self.t == TypeEnum.KRR:
            return f"krr({repr(self.aS())})"
        return f"ks({repr(self.aS())})"


class KFnAtom(KObj):
    def __init__(
        self,
        context: KContext = DEFAULT_CONTEXT,
        prelude: bytes = b"",
        data: bytes = b"",
    ) -> None:
        super().__init__(TypeEnum.FN, context)
        self.prelude: bytes = prelude
        self.data: bytes = data

    def aS(self) -> str:
        return self.data.decode("ascii")

    def ss(self, s: str) -> KObj:
        self.data = bytes(s, "ascii")
        return self

    def __repr__(self) -> str:
        return f"KFnAtom(prelude={repr(self.prelude)}, data={repr(self.data)})"

    # serialisation
    def _databytes(self) -> bytes:
        return (
            super()._databytes()
            + self.prelude
            + struct.pack("i", len(self.data))
            + self.data
        )

    def _paysz(self) -> int:
        return super()._paysz() + len(self.data) + 4 + len(self.prelude)

    def frombytes(self, data: bytes, offset: int) -> Tuple[KObj, int]:
        # guesswork: k lambdas use a 4 byte prelude, q 3 bytes
        prelude_len = {0: 3, 104: 4, 113: 4}[data[offset]]

        sz = struct.unpack("i", data[offset + prelude_len : offset + prelude_len + 4])[
            0
        ]
        self.prelude = data[offset : offset + prelude_len]
        self.data = data[offset + prelude_len + 4 : offset + prelude_len + 4 + sz]
        return self, offset + len(self.prelude) + 4 + len(self.data)


class KOpAtom(KObj):
    def __init__(
        self,
        context: KContext = DEFAULT_CONTEXT,
    ) -> None:
        super().__init__(TypeEnum.OP, context)
        self.op: int = 0

    def aJ(self) -> int:
        return self.op

    # serialisation
    def _databytes(self) -> bytes:
        return super()._databytes() + struct.pack("<b", self.op)

    def _paysz(self) -> int:
        return super()._paysz() + 1

    def frombytes(self, data: bytes, offset: int) -> Tuple[KObj, int]:
        self.op = data[offset]
        return self, offset + 1


class KRangedType(KObj):
    def frombytes(self, data: bytes, offset: int) -> Tuple[KObj, int]:
        attrib, sz = struct.unpack_from("<BI", data, offset=offset)
        self.attrib = attrib
        logger.debug(f" frombytes for ranged type attrib={attrib} sz={sz}")
        return self._ranged_frombytes(sz, data, offset + 5)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        raise self._te()


class KByteArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KG, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._g = array.array("B", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 1 * len(self._g)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self._g)) + struct.pack(
            f"<{len(self._g)}B", *self._g
        )

    def __repr__(self) -> str:
        if self.t == TypeEnum.KB:
            parts = ", ".join(repr(r) for r in self.kB())
            return f"ktnb({parts})"
        elif self.t == TypeEnum.KG:
            parts = ", ".join(repr(r) for r in self.kG())
            return f"ktni({self._te_repr()}, {parts})"
        else:
            return super().__repr__()

    def kG(self) -> "MutableSequence[int]":
        return self._g

    def kB(self) -> "MutableSequence[bool]":
        return BoolByteAdaptor(self._g)

    def __len__(self) -> int:
        return len(self._g)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._g = array.array("B", data[offset : offset + sz])
        return self, offset + sz


class KShortArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KH, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._h = array.array("h", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 2 * len(self._h)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self._h)) + struct.pack(
            f"<{len(self._h)}H", *self._h
        )

    def __repr__(self) -> str:
        parts = ", ".join(repr(r) for r in self.kH())
        return f"ktni({self._te_repr()}, {parts})"

    def kH(self) -> "MutableSequence[int]":
        return self._h

    def __len__(self) -> int:
        return len(self._h)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._h = array.array("h", data[offset : offset + 2 * sz])
        return self, offset + 2 * sz


class KIntArray(KRangedType):
    # python doesn't have a reliable 32-bit array type, "l" can either be 32 or 64 bits
    # which breaks direct cast-style deserialising. So when l is 64 bits we need to
    # iteratively unpack with unpack_from. Somewhat annoying given the other types map
    # cleanly.
    def __init__(self, t: int = TypeEnum.KI, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._i = array.array("l", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 4 * len(self._i)

    def _databytes(self) -> bytes:
        if self._i.itemsize == 4:  # array type 'l' seems to store as 8 bytes
            pi = self._i.tobytes()
        else:
            pi = struct.pack(f"<{len(self._i)}I", *self._i)
        return struct.pack("<bBI", self.t, self.attrib, len(self._i)) + pi

    def __repr__(self) -> str:
        parts = ", ".join(repr(r) for r in self.kI())
        return f"ktni({self._te_repr()}, {parts})"

    def kI(self) -> "MutableSequence[int]":
        return self._i

    def __len__(self) -> int:
        return len(self._i)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
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
    def _paysz(self) -> int:
        return 2 + 4 + sum([len(self.context.lookup_bytes(j)) for j in self._i])

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._i))]
        for j in self._i:
            parts.append(self.context.lookup_bytes(j))
        return b"".join(parts)

    def __repr__(self) -> str:
        strs = ", ".join(repr(s) for s in self.kS())
        return f"ktns({strs})"

    def kS(self) -> "MutableSequence[str]":
        return SymIntAdaptor(self._i, self.context)

    def appendS(self, *ss: str) -> KObj:
        for s in ss:
            j = self.context.ss(s)
            self._i.append(j)
        return self

    def __len__(self) -> int:
        return len(self._i)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._i = array.array("l", [0] * sz)
        for i in range(sz):
            bs = data.index(b"\x00", offset) - offset
            s = data[offset : offset + bs].decode("utf-8")
            d = self.context.ss(s)
            self._i[i] = d
            offset += bs + 1
        return self, offset


class KLongArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KJ, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._j = array.array("q", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 8 * len(self._j)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self._j)) + struct.pack(
            f"<{len(self._j)}q", *self._j
        )

    def __repr__(self) -> str:
        parts = ", ".join(repr(j) for j in self._j)
        return f"ktni({self._te_repr()}, {parts})"

    def kJ(self) -> "MutableSequence[int]":
        return self._j

    def __len__(self) -> int:
        return len(self._j)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._j = array.array("q", data[offset : offset + 8 * sz])
        return self, offset + 8 * sz


class KFloatArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KF, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._e = array.array("f", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 4 * len(self._e)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self._e)) + struct.pack(
            f"<{len(self._e)}f", *self._e
        )

    def __repr__(self) -> str:
        parts = ", ".join(repr(f) for f in self._e)
        return f"ktnf({self._te_repr()}, {parts})"

    def kE(self) -> "MutableSequence[float]":
        return self._e

    def __len__(self) -> int:
        return len(self._e)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._e = array.array("f", data[offset : offset + 4 * sz])
        return self, offset + 4 * sz


class KDoubleArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KF, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._f = array.array("d", [0] * sz)

    def _paysz(self) -> int:
        return 2 + 4 + 8 * len(self._f)

    def _databytes(self) -> bytes:
        return struct.pack("<bBI", self.t, self.attrib, len(self._f)) + struct.pack(
            f"<{len(self._f)}d", *self._f
        )

    def __repr__(self) -> str:
        parts = ", ".join(repr(f) for f in self._f)
        return f"ktnf({self._te_repr()}, {parts})"

    def kF(self) -> "MutableSequence[float]":
        return self._f

    def __len__(self) -> int:
        return len(self._f)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        self._f = array.array("d", data[offset : offset + 8 * sz])
        return self, offset + 8 * sz


class KCharArray(KRangedType):
    def __init__(self, t: int = TypeEnum.KC, sz: int = 0, attr: int = 0) -> None:
        super().__init__(t, attr=attr)
        self._c: array.array[str] = array.array("u", [" "] * sz)

    def _paysz(self) -> int:
        bs = self._c.tounicode().encode("utf-8")
        return 2 + 4 + 1 * len(bs)

    def _databytes(self) -> bytes:
        bs = self._c.tounicode().encode("utf-8")
        return struct.pack("<bBI", self.t, self.attrib, len(bs)) + bs

    def __repr__(self) -> str:
        return f"cv({repr(self.aS())})"

    def kC(self) -> array.array:  # type: ignore[type-arg]
        return self._c

    def aS(self) -> str:
        return self._c.tounicode()

    def __len__(self) -> int:
        return len(self._c)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        s = data[offset : offset + sz].decode("utf-8")
        self._c = array.array("u", [])
        self._c.fromunicode(s)
        return self, offset + sz


class KObjArray(KRangedType):
    def __init__(self, t: int = TypeEnum.K) -> None:
        super().__init__(0)
        self._k: List[KObj] = []

    def _paysz(self) -> int:
        # sum sizes nested ks
        return 2 + 4 + sum([ko._paysz() for ko in self._k])

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._k))]
        parts.extend(ko._databytes() for ko in self._k)
        return b"".join(parts)

    def __repr__(self) -> str:
        return "kk(" + (", ".join([repr(k) for k in self._k])) + ")"

    def kK(self) -> "MutableSequence[KObj]":
        return self._k

    def __len__(self) -> int:
        return len(self._k)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        for i in range(sz):
            obj, offset = _d9_unpackfrom(data, offset)
            self._k.append(obj)
        return self, offset


class KUUIDArray(KRangedType):
    def __init__(self, t: int = TypeEnum.UU, sz: int = 0, attr: int = 0) -> None:
        super().__init__(TypeEnum.UU)
        self._u: List[uuid.UUID] = [uuid.UUID(int=0)] * sz

    def _paysz(self) -> int:
        # sum sizes nested ks
        return 2 + 4 + 16 * len(self._u)

    def _databytes(self) -> bytes:
        parts = [struct.pack("<bBI", self.t, self.attrib, len(self._u))]
        parts.extend(uu.bytes for uu in self._u)
        return b"".join(parts)

    def __repr__(self) -> str:
        parts = ", ".join([repr(k) for k in self._u])
        return f"ktnu({parts})"

    def kU(self) -> "MutableSequence[uuid.UUID]":
        return self._u

    def __len__(self) -> int:
        return len(self._u)

    def _ranged_frombytes(self, sz: int, data: bytes, offset: int) -> Tuple[KObj, int]:
        for i in range(sz):
            start = offset + i * 16
            self._u.append(uuid.UUID(bytes=data[start : start + 16]))
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
    offset = 8
    if flags == 1:
        data = decompress(data[8:])
        offset = 0
        msglen = len(data)
    elif flags != 0:
        raise ValueError(
            f"unknown payload flags={flags} - not yet implemented, please open an Issue. Buffer: {data[:16].hex()}"
        )
    try:
        k, pos = _d9_unpackfrom(data, offset=offset)
        if pos != msglen:
            raise Exception(f"Final position at {pos} expected {msglen}")
    except ValueError as ve:
        raise Exception(f"While unpacking buffer {data[:100]!r}") from ve
    return k


def _d9_unpackfrom(data: bytes, offset: int) -> Tuple[KObj, int]:
    (t,) = struct.unpack_from("<b", data, offset=offset)
    offset += 1
    logger.debug(
        f" at offset {offset}/{len(data)} unpacking type {tn(t)} peek {data[offset : offset + 100]!r}"
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
    elif t == TypeEnum.FN:
        return KFnAtom().frombytes(data, offset)
    elif t == TypeEnum.OP:
        return KOpAtom().frombytes(data, offset)
    elif t >= 20 and t < 30:
        return VECTOR_CONSTUCTORS[TypeEnum.KJ](t).frombytes(data, offset)
    raise ValueError(f"Unable to d9 unpack t={t}")


# atom constructors
def ka(t: Union[int, TypeEnum]) -> KObj:
    return KObjAtom(t)


def kb(b: bool) -> KObj:
    return KObjAtom(-TypeEnum.KB).b(b)


def kc(c: str) -> KObj:
    return KObjAtom(-TypeEnum.KC).c(c)


def kg(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KG).g(i)


def kh(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KH).h(i)


def ki(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KI).i(i)


def ke(f: float) -> KObj:
    return KObjAtom(-TypeEnum.KE).f(f)


def kf(f: float) -> KObj:
    return KObjAtom(-TypeEnum.KF).f(f)


def kj(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KJ).j(i)


def kp(i: int) -> KObj:
    return KObjAtom(-TypeEnum.KP).j(i)


def ks(s: str) -> KObj:
    return KSymAtom(-TypeEnum.KS).ss(s)


def kuu(uu: uuid.UUID) -> KObj:
    return KObjAtom(-TypeEnum.UU).uu(uu)


# vector constructors
VECTOR_CONSTUCTORS: Dict[TypeEnum, Type[KObj]] = {
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


def ktn(t: TypeEnum, sz: int = 0, attr: AttrEnum = AttrEnum.NONE) -> KObj:
    if t == TypeEnum.K:
        if sz > 0:
            raise ValueError("ktn K can only be empty at initialisation")
        return KObjArray(t)
    try:
        return VECTOR_CONSTUCTORS[t](t, sz=sz, attr=attr)
    except KeyError:
        raise ValueError(f"ktn for type {tn(t)}")


def kk(*objs: KObj) -> KObj:
    for o in objs:
        if not isinstance(o, KObj):
            raise ValueError(f"not KObj: {o}")
    k = ktn(TypeEnum.K)
    k.kK().extend(objs)
    return k


def cv(s: str) -> KObj:
    k = ktn(TypeEnum.KC)
    k.kC().fromunicode(s)
    return k


class KDict(KObj):
    def __init__(self, kkeys: KObj, kvalues: KObj, t: TypeEnum = TypeEnum.XD):
        if len(kkeys) != len(kvalues):
            raise ValueError("dict keys and values must be same length")
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

    def __repr__(self) -> str:
        suffix = ", sorted=True" if self.t == TypeEnum.SD else ""
        return f"xd({repr(self._kkey)}, {repr(self._kvalue)}{suffix})"

    def __len__(self) -> int:
        return len(self._kkey)

    def kkey(self) -> KObj:
        return self._kkey

    def kvalue(self) -> KObj:
        return self._kvalue

    def kS(self) -> "MutableSequence[str]":
        # key names
        if self._kkey.t == TypeEnum.KS:
            return self.kkey().kS()
        elif self._kkey.t == TypeEnum.K:
            return [k.aS() for k in self.kkey().kK()]

        raise KeyError(f"Keys as strings not possible on {self._kkey._tn()}")

    def __getitem__(self, item: Union[int, str]) -> KObj:
        if not isinstance(item, str):
            raise ValueError("cannot index dict by int")
        if self._kkey.t == TypeEnum.KS:
            try:
                idx = self.kkey().kS().index(item)
                return self.kvalue().kK()[idx]
            except ValueError:
                raise KeyError(f"Key not found {item}")
        elif self._kkey.t == TypeEnum.K:
            # check for any nested atomic symbol keys within the K object array
            for idx in range(len(self._kkey)):
                k = self._kkey.kK()[idx]
                if k.t == -TypeEnum.KS:
                    if k.aS() == item:
                        return self.kvalue().kK()[idx]
            raise KeyError(f"Key not found {item}")

        raise KeyError(f"Key lookup not possible on {self._kkey._tn()}")


def atomic_from_vect_index(v: KObj, index: int) -> KObj:
    # tricky: get index item from v
    if v.t == TypeEnum.KJ:
        return kj(v.kJ()[index])
    elif v.t == TypeEnum.KI:
        return ki(v.kI()[index])
    elif v.t == TypeEnum.KH:
        return kh(v.kH()[index])
    elif v.t == TypeEnum.KB:
        return kb(v.kB()[index])
    elif v.t == TypeEnum.UU:
        return kuu(v.kU()[index])
    elif v.t == TypeEnum.KG:
        return kg(v.kG()[index])
    elif v.t == TypeEnum.KF:
        return kf(v.kF()[index])
    elif v.t == TypeEnum.KG:
        return kg(v.kG()[index])
    elif v.t == TypeEnum.KC:
        return kc(v.kC()[index])
    elif v.t == TypeEnum.KS:
        # symbol index
        si = v.kI()[index]
        # TODO: could improve this further since ks repeats lookup
        return ks(v.context.lookup_str(si))
    elif v.t == TypeEnum.KP:
        return kp(v.kJ()[index])
    elif v.t == TypeEnum.K:
        return v.kK()[index]
    else:
        raise ValueError(f"no box/unbox defined for {v}")


class KFlip(KObj):
    def __init__(self, kd: KObj, sorted: bool = False):
        if kd.t != TypeEnum.XD:
            raise ValueError(f"can only flip a dict, not {kd._tn()}")
        if len(kd.kkey()) == 0:
            raise ValueError("must have >0 columns")
        if kd.kvalue().t != TypeEnum.K:
            raise ValueError("must have K vector holding cols")
        if kd.kkey().t != TypeEnum.KS:
            raise ValueError("dict key must be S vector of column names")
        if len(kd.kvalue().kK()) == 0:
            raise ValueError("cannot have zero columns")
        # check all columns have same length
        c = len(kd.kvalue().kK()[0])
        for col in kd.kvalue().kK():
            if len(col) != c:
                raise ValueError("column length inconsistent")

        attr = AttrEnum.NONE
        if sorted:
            attr = AttrEnum.SORTED
        super().__init__(TypeEnum.XT, attr=attr)
        self._kvalue = kd

    def _paysz(self) -> int:
        return 2 + self._kvalue._paysz()

    def _databytes(self) -> bytes:
        return struct.pack("<bB", self.t, self.attrib) + self._kvalue._databytes()

    def __repr__(self) -> str:
        # TODO: flip attr
        return f"xt({repr(self._kvalue)})"

    def kvalue(self) -> KObj:
        return self._kvalue

    def __len__(self) -> int:
        # the length of a table is not the length of it's contained dictionary, as that is the number of columns.
        # we need to read-through to the first columns data.
        assert self.kvalue().t == TypeEnum.XD
        tdict_values = self.kvalue().kvalue()
        if len(tdict_values) == 0:
            raise ValueError("flip contains a dict with no values, so has no length")
        assert tdict_values.t == TypeEnum.K
        first_col = tdict_values.kK()[0]
        return len(first_col)

    def __getitem__(self, item: Union[int, str]) -> KObj:
        if isinstance(item, int):
            if item >= len(self):
                raise IndexError()
            d = self.kvalue()
            vals = []
            for k in self.kS():
                v = self[k]
                a = atomic_from_vect_index(v, item)
                vals.append(a)

            return xd(d.kkey(), kk(*vals))
        elif isinstance(item, str):
            try:
                idx = self.kS().index(item)
                return self.kK()[idx]
            except ValueError:
                raise KeyError(f"Column not found {item}")

    def kS(self) -> "MutableSequence[str]":
        # column names
        return self.kvalue().kkey().kS()

    def kK(self) -> "MutableSequence[KObj]":
        # column values
        return self.kvalue().kvalue().kK()


def krr(msg: str) -> KObj:
    return KSymAtom(TypeEnum.KRR).ss(msg)


kNil = ka(TypeEnum.NIL)


class KException(Exception):
    pass


def xd(kkeys: KObj, kvalues: KObj, sorted: bool = False) -> KDict:
    if sorted:
        return KDict(kkeys, kvalues, TypeEnum.SD)
    return KDict(kkeys, kvalues)


def xt(kd: KDict, sorted: bool = False) -> KFlip:
    return KFlip(kd, sorted=sorted)
