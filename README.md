![Python versions](https://img.shields.io/pypi/pyversions/aiokdb.svg) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/TeaEngineering/aiokdb/check.yml) [![PyPI version](https://badge.fury.io/py/aiokdb.svg)](https://badge.fury.io/py/aiokdb)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb?ref=badge_shield)

# aiokdb
Python asyncio connector to KDB. Pure python, so does not depend on the `k.h` bindings or kdb shared objects, or numpy/pandas. Fully type hinted to comply with `PEP-561`. No non-core dependencies, and tested on Python 3.8 - 3.12.

## Peer review & motivation

[qPython](https://github.com/exxeleron/qPython) is a widely used library for this task that maps KDB tables to Pandas Dataframes. Sometimes a dependency on numpy/pandas is not desired, or the dynamic type mapping might be unwanted.

This library takes a different approach and aims to replicate using the KDB C-library functions, ie. being 100% explicit about KDB types. It was built working from the publicly documented [Serialisation Examples](https://code.kx.com/q/kb/serialization/) and [C API for kdb+](https://code.kx.com/q/wp/capi/) pages. Users might also need to be familiar with [k.h](https://github.com/KxSystems/ffi/blob/master/include/k.h).


```python
% python
Python 3.9.22 (main, Apr  8 2025, 15:21:55)
>>> from aiokdb import *
>>> from aiokdb.extras import ktns, ktni, ktnb
>>> x = xt(xd(ktns("hi", "there"), kk(ktni(TypeEnum.KI, 45, 56), ktnb(False, True))))
>>> x  # shows repr() output
xt(xd(ktns('hi', 'there'), kk(ktni(TypeEnum.KI, 45, 56), ktnb(False, True))))
>>> from aiokdb.format import AsciiFormatter
>>> print(AsciiFormatter().format(x))
hi there
--------
45 0
56 1
>>> len(x)
2
>>> x[0]
xd(ktns('hi', 'there'), kk(ki(45), kb(False)))
>>> print(AsciiFormatter().format(x[1]))
hi   | 56i
there| 1
>>> x[2]
IndexError
```

Basic RPC, using blocking sockets:

```python
# run ./q -p 12345 &

from aiokdb.socket import khpu

h = khpu("localhost", 12345, "kdb:pass")

# if remote returns Exception, it is raised here, unless khpu(..., raise_krr=False)
result = h.k("2.0+3.0")

assert result.aF() == 5.0

result.aJ() # raises ValueError: wrong type KF (-9) for aJ
```

The `result` object is a K-like Python object (a `KObj`), having the usual signed integer type available as `result.type`. Accessors for the primitive types are prefixed with an `a` and check at runtime that the accessor is appropriate for the stored type (`.aI()`, `.aJ()`, `.aH()`, `.aF()` etc.). Atoms store their value to a `bytes` object irrespective of the type, and encode/decode on demand. Atomic values can be set with (`.i(3)`, `.j(12)`, `.ss("hello")`).

Arrays are implemented with subtypes that use [Python's native arrays module](https://docs.python.org/3/library/array.html) for efficient array types. The `MutableSequence` arrays are returned using the usual array accessor functions `.kI()`, `.kB()`, `.kS()` etc.

```
 kdb type name       python    python      python       python  python
  n   c              TypeEnum  accessor    setter       create  type
 --------------------------------------------------------------------------------------
 -19  t   time       -KT       -           -            -       -
 -18  v   second     -KV       -           -            -       -
 -17  u   minute     -KU       -           -            -       -
 -16  n   timespan   -KN       -           -            -       -
 -15  z   datetime   -KZ       -           -            -       -
 -14  d   date       -KD       -           -            -       -
 -13  m   month      -KM       -           -            -       -
 -12  p   timestamp  -KP       -           -            -       -
 -11  s   symbol     -KS       .aS()       .ss("sym")   ks()    str
 -10  c   char       -KC       .aC()       .c("c")      kc()    str (len 1)
  -9  f   float      -KF       .aF()       .f(6.1)      kf()    float
  -8  e   real       -KE       .aE()       .f(6.2)      ke()    float
  -7  j   long       -KJ       .aJ()       .j(7)        kj()    int
  -6  i   int        -KI       .aI()       .i(6)        ki()    int
  -5  h   short      -KH       .aH()       .h(5)        kh()    int
  -4  x   byte       -KG       .aG()       .g(4)        kg()    int
  -2  g   guid       -UU       .aU()       .uu(UUID())  kuu()   uuid.UUID
  -1  b   boolean    -KB       .aB()       .b(True)     kb()    bool
   0  *   list        K        .kK()       -            kk()    MutableSequence[KObj]
   1  b   boolean     KB       .kB()       -            ktnb()  MutableSequence[bool]
   2  g   guid        UU       .kU()       -            ktnu()  MutableSequence[uuid.UUID]
   4  x   byte        KG       .kG()       -            ktni()  MutableSequence[int]
   5  h   short       KH       .kH()       -            ktni()  MutableSequence[int]
   6  i   int         KI       .kI()       -            ktni()  MutableSequence[int]
   7  j   long        KJ       .kJ()       -            ktni()  MutableSequence[int]
   8  e   real        KE       .kE()       -            ktnf()  MutableSequence[float]
   9  f   float       KF       .kF()       -            ktnf()  MutableSequence[float]
  10  c   char        KC       .kC(),.aS() -            cv()    array.array, str
  11  s   symbol      KS       .kS()       -            ktns()  MutableSequence[str]
  12  p   timestamp   KP       -           -            -       -
  13  m   month       KM       -           -            -       -
  14  d   date        KD       -           -            -       -
  15  z   datetime    KZ       -           -            -       -
  16  n   timespan    KN       -           -            -       -
  17  u   minute      KU       -           -            -       -
  18  v   second      KV       -           -            -       -
  19  t   time        KT       -           -            -       -
  98      flip        XT       .kkey(), .kvalue()       xt()    KObj, KObj
  99      dict        XD       .kkey(), .kvalue()       xd()    KObj, KObj
 100      function    FN       -           -            KFnAtom       -
 101  ::  nil        NIL       -           -            kNil    -
 127      `s#dict     SD       .kkey(), .kvalue()       -       KObj, KObj
-128  '   err        KRR       .aS()       .ss()        krr()   str
```

Serialisation is handled by the `b9` function, which encodes a `KObj` to a python `bytes`, and the `d9` function which takes a `bytes` and returns a `KObj`.

Calling `repr()` on  `KObj` returns a string representation that, when passed to `eval()`, will exactly recreate the `KObj`. This may be an expensive operation for deeply nested or large tables.

* Atoms are created by `ka`, `kb`, `ku`, `kg`, `kh`, `ki`, `kj`, `ke`, `kf`, `kc`, `ks`, `kt`, `kd`, `kz`, `ktj`
* Vectors from python primitives with `ktnu`, `ktni`, `ktnb`, `ktnf`, `ktns`, passing desired `TypeEnum` value as the first argument.
* Mixed-type objects lists with `kk`.
* Dictionaries with `xd` and tables with `xt`.

Python manages garbage collection, so none of the reference counting primitives exist, i.e. `k.r` and functions `r1`, `r0` and `m9`, `setm`.

## Asyncio

Both kdb client and server *protocols* are implemented using asyncio, and can be tested back-to-back.
For instance running `python -m aiokdb.server` and then `python -m aiokdb.client` will connect together using KDB IPC. However since there is no _interpreter_ (and the default server does not handle any commands) the server will return an `nyi` error to all queries. To implement a partial protocol for your own application, subclass `aiokdb.server.ServerContext` and implement `on_sync_request()`, `on_async_message()`, and perhaps `check_login()`.

## Command Line Interface

Usable command line client support for connecting to a remote KDB instance (using python `asyncio`, and `prompt_toolkit` for line editing and history) is built into the package:

```bash
$ pip install aiokdb prompt_toolkit
$ ./q -p 12345 &
$ python -m aiokdb.cli --host localhost --port 12345
(eval) > ([s:7 6 0Nj]x:3?0Ng;y:2)
s| x                                    y
-|---------------------------------------
7| 409031f3-b19c-6770-ee84-6e9369c98697 2
6| 52cb20d9-f12c-9963-2829-3c64d8d8cb14 2
 | cddeceef-9ee9-3847-9172-3e3d7ab39b26 2
(eval) > 4 5 6!(`abc`def;til 300;(3 4!`a`b))
4| abc def
5| 0 1 2 ... 297 298 299
6| KDict
(eval) > [ctrl-D]
$
```

## Formatting

We implement `repr(KObj)` as a recursively descending, type explicit formatter, suitable for logging complex or unknown responses, albeit whose size could be very large for large datasets. The implementation of `str(KObj)` is non-descending, and will always be constant time/space irrespective of the payload.

Text formatting (as shown above) is controlled by `aiokdb.format.ASCIIFormatter`, which looks inside a `KObj` to render `XD`, `SD`, `XT` types in tabular form containing atom and vector values. Nested complex types ie. dictionary or tables render as the literals `KDict` or `KFlip`, so the outer form is preserved. The output can be bounded by passing arguments to the formatter, e.g. `ASCIIFormatter(width=120, height=20)`.

Finally `aiokdb.format.HtmlFormatter` is suitable for dashboards etc and can be trivially added to web frameworks as a 'template tag' or similar.

## QDB Files
Ordinary `.qdb` files written with set can be read by `kfromfile` or written by `ktofile`:

```python
>>> from aiokdb.files import kfromfile, ktofile
>>> k = kfromfile('test_qdb0/test.qdb')
>>> k
<aiokdb.KObjArray object at 0x7559136d8230>
>>> from aiokdb.format import AsciiFormatter
>>> fmt = AsciiFormatter()
>>> print(fmt.format(k))
[5, hello]
```

Ordinarily `k` is dictionary representing a KDB namespace containing other objects.

There is no support for splayed or partitioned datasets, however the primitives are (or, at least used to be) the same so this would be possible.

## Char vectors
Char vectors are created with `cv` and symbols with `ks`, both from a string. They both support returning immutable python strings with accessor `.aS()`. You can also return a char vector as a *mutable* string (writing back to the KObj) as an array of characters with `.kC()`, however this is an unusual pattern to use in python:

```python
>>> x = cv('hello')
>>> x.kC()
array('u', 'hello')
>>> x.aS()
'hello'
>>> x.kC()[3] = 'u'; x
cv('heluo')
# With symbol
>>> ks("hello").aS()
'hello'
>>> ks("hello").kC()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "lib/python3.9/site-packages/aiokdb/__init__.py", line 262, in kC
    raise self._te()
aiokdb.WrongTypeForOperationError: Not available for KS (-11)
```

## Tests
The library has extensive test coverage, however de-serialisation of certain (obscure) KObj may not be fully supported yet. PR's welcome. All tests are pure python except for those in `test/test_rpc.py`, which will use a real KDB server to test against if you set the `KDB_PYTEST_SERVICE` environment variable (to a URL of the form `kdb://user:password@hostname:port`), otherwise that test is skipped.

* Formatting with `ruff check .`
* Formatting with `ruff format .`
* Check type annotations with `mypy --strict .`
* Run `pytest .` in the root directory


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb?ref=badge_large)